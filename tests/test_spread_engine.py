"""
Tests for spread calculation and confidence scoring.

Covers: both directions, fee/slippage subtraction, edge cases,
confidence factor scoring, zero/negative spreads.
"""

from datetime import datetime, timezone, timedelta
from decimal import Decimal

import pytest

from models.snapshot import MarketSnapshot
from spread_engine.calculator import (
    calculate_spread,
    _estimate_fees,
    _estimate_slippage,
    _calc_one_direction,
)
from spread_engine.confidence import (
    score_freshness,
    score_liquidity,
    score_volume,
    score_spread_magnitude,
    calculate_confidence,
)


NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def make_snap(
    exchange: str = "binance",
    bid: str = "1.2000",
    ask: str = "1.2100",
    bid_size: str = "1000",
    ask_size: str = "800",
    age_ms: int = 100,
    **kwargs,
) -> MarketSnapshot:
    """Helper to create snapshots with configurable age."""
    local_ts = NOW - timedelta(milliseconds=age_ms)
    defaults = {
        "canonical_symbol": "APE-USDT-PERP",
        "exchange": exchange,
        "bid": Decimal(bid),
        "ask": Decimal(ask),
        "bid_size": Decimal(bid_size),
        "ask_size": Decimal(ask_size),
        "local_ts": local_ts,
    }
    defaults.update(kwargs)
    return MarketSnapshot(**defaults)


# ---------------------------------------------------------------------------
# Spread calculation — both directions
# ---------------------------------------------------------------------------

class TestCalculateSpread:

    def test_positive_spread_one_direction(self):
        """A.ask < B.bid → buy on A, sell on B is profitable."""
        snap_a = make_snap(exchange="binance", ask="1.2000")
        snap_b = make_snap(exchange="gate", bid="1.2100")

        opps = calculate_spread(snap_a, snap_b, now=NOW)

        # Only one direction should be positive
        buy_on_a = [o for o in opps if o.buy_exchange == "binance"]
        assert len(buy_on_a) == 1
        opp = buy_on_a[0]
        assert opp.buy_exchange == "binance"
        assert opp.sell_exchange == "gate"
        # gross_spread = 1.2100 - 1.2000 = 0.0100
        assert opp.gross_spread == Decimal("0.0100")

    def test_both_directions_when_crossed(self):
        """If both directions have positive gross spread (unusual but possible)."""
        # A: bid=1.22, ask=1.20 (inverted — unlikely but tests the logic)
        # B: bid=1.23, ask=1.19
        snap_a = make_snap(exchange="binance", bid="1.2200", ask="1.1900")
        snap_b = make_snap(exchange="gate", bid="1.2300", ask="1.1800")

        opps = calculate_spread(snap_a, snap_b, now=NOW)
        assert len(opps) == 2

    def test_no_spread_when_prices_equal(self):
        snap_a = make_snap(exchange="binance", bid="1.2000", ask="1.2000")
        snap_b = make_snap(exchange="gate", bid="1.2000", ask="1.2000")

        opps = calculate_spread(snap_a, snap_b, now=NOW)
        assert len(opps) == 0

    def test_no_spread_when_negative(self):
        """A.ask > B.bid and B.ask > A.bid → no opportunity."""
        snap_a = make_snap(exchange="binance", bid="1.1900", ask="1.2100")
        snap_b = make_snap(exchange="gate", bid="1.1950", ask="1.2050")

        opps = calculate_spread(snap_a, snap_b, now=NOW)
        assert len(opps) == 0

    def test_symbol_mismatch_returns_empty(self):
        snap_a = make_snap(canonical_symbol="APE-USDT-PERP")
        snap_b = make_snap(canonical_symbol="XRP-USDT-PERP")

        opps = calculate_spread(snap_a, snap_b, now=NOW)
        assert len(opps) == 0

    def test_cross_quote_same_base_works(self):
        """USDT vs USDC with same base asset should calculate spreads."""
        snap_a = make_snap(exchange="binance", canonical_symbol="APE-USDT-PERP", ask="1.2000")
        snap_b = make_snap(exchange="hyperliquid", canonical_symbol="APE-USDC-PERP", bid="1.2100")

        opps = calculate_spread(snap_a, snap_b, now=NOW)
        buy_on_a = [o for o in opps if o.buy_exchange == "binance"]
        assert len(buy_on_a) == 1
        assert buy_on_a[0].gross_spread == Decimal("0.0100")

    def test_cross_quote_different_base_rejected(self):
        """Different base assets should still be rejected even with equivalent quotes."""
        snap_a = make_snap(canonical_symbol="APE-USDT-PERP")
        snap_b = make_snap(canonical_symbol="DOGE-USDC-PERP")

        opps = calculate_spread(snap_a, snap_b, now=NOW)
        assert len(opps) == 0

    def test_gross_spread_bps_formula(self):
        """Verify: gross_spread_bps = (gross_spread / buy_ask) * 10000"""
        snap_a = make_snap(exchange="binance", ask="1.0000")
        snap_b = make_snap(exchange="gate", bid="1.0050")

        opps = calculate_spread(snap_a, snap_b, now=NOW)
        opp = [o for o in opps if o.buy_exchange == "binance"][0]

        # gross_spread = 1.0050 - 1.0000 = 0.0050
        # gross_spread_bps = (0.0050 / 1.0000) * 10000 = 50.0
        assert opp.gross_spread_bps == Decimal("50.0000")

    def test_net_spread_subtracts_costs(self):
        snap_a = make_snap(exchange="binance", ask="1.0000")
        snap_b = make_snap(exchange="gate", bid="1.0050")

        opps = calculate_spread(snap_a, snap_b, now=NOW)
        opp = [o for o in opps if o.buy_exchange == "binance"][0]

        # Net spread should be less than gross spread
        assert opp.net_spread < opp.gross_spread
        assert opp.estimated_fees > 0
        assert opp.estimated_slippage > 0

    def test_net_spread_can_be_negative(self):
        """Very small gross spread should result in negative net after costs."""
        snap_a = make_snap(exchange="binance", ask="1.0000")
        snap_b = make_snap(exchange="gate", bid="1.0001")  # tiny spread

        opps = calculate_spread(snap_a, snap_b, now=NOW)
        opp = [o for o in opps if o.buy_exchange == "binance"][0]

        # Costs likely exceed 0.0001 gross spread
        assert opp.net_spread < 0

    def test_includes_funding_rates(self):
        snap_a = make_snap(exchange="binance", ask="1.0000",
                          funding_rate=Decimal("0.0001"))
        snap_b = make_snap(exchange="gate", bid="1.0050",
                          funding_rate=Decimal("-0.0002"))

        opps = calculate_spread(snap_a, snap_b, now=NOW)
        opp = [o for o in opps if o.buy_exchange == "binance"][0]
        assert opp.buy_funding_rate == Decimal("0.0001")
        assert opp.sell_funding_rate == Decimal("-0.0002")

    def test_includes_sizes(self):
        snap_a = make_snap(exchange="binance", ask="1.0000", ask_size="500")
        snap_b = make_snap(exchange="gate", bid="1.0050", bid_size="300")

        opps = calculate_spread(snap_a, snap_b, now=NOW)
        opp = [o for o in opps if o.buy_exchange == "binance"][0]
        assert opp.buy_ask_size == Decimal("500")
        assert opp.sell_bid_size == Decimal("300")

    def test_data_age_is_max_of_both(self):
        snap_a = make_snap(exchange="binance", ask="1.0000", age_ms=200)
        snap_b = make_snap(exchange="gate", bid="1.0050", age_ms=800)

        opps = calculate_spread(snap_a, snap_b, now=NOW)
        opp = [o for o in opps if o.buy_exchange == "binance"][0]
        assert opp.data_age_ms == 800


# ---------------------------------------------------------------------------
# Fee and slippage estimation
# ---------------------------------------------------------------------------

class TestFeeEstimation:

    def test_binance_gate_fees(self):
        mid = Decimal("1.0000")
        fees = _estimate_fees("binance", "gate", mid)
        # taker_buy(binance) = 0.0004, maker_sell(gate) = 0.00015
        # fees = (0.0004 + 0.00015) * 1.0 * 2 = 0.0011
        assert fees == Decimal("0.00110")

    def test_unknown_exchange_uses_default(self):
        fees = _estimate_fees("unknown", "unknown", Decimal("1.0"))
        # defaults: 0.0005, 0.0005
        # fees = (0.0005 + 0.0005) * 1.0 * 2 = 0.0020
        assert fees == Decimal("0.0020")

    def test_slippage(self):
        slippage = _estimate_slippage(Decimal("1.0000"))
        # SLIPPAGE_FACTOR = 0.0001
        assert slippage == Decimal("0.00010000")


# ---------------------------------------------------------------------------
# Confidence scoring
# ---------------------------------------------------------------------------

class TestConfidenceScoring:

    def test_freshness_full_score(self):
        snap = make_snap(age_ms=100)
        score = score_freshness(snap, snap, now=NOW)
        assert score == Decimal("1.0")

    def test_freshness_degraded(self):
        snap = make_snap(age_ms=1000)  # between 500ms and 2000ms
        score = score_freshness(snap, snap, now=NOW)
        assert Decimal("0") < score < Decimal("1.0")

    def test_freshness_zero(self):
        snap = make_snap(age_ms=3000)
        score = score_freshness(snap, snap, now=NOW)
        assert score == Decimal("0.0")

    def test_freshness_uses_older_snapshot(self):
        snap_fresh = make_snap(age_ms=100)
        snap_stale = make_snap(age_ms=1500)
        score = score_freshness(snap_fresh, snap_stale, now=NOW)
        # Should be based on the 1500ms age, not the 100ms
        assert score < Decimal("0.5")

    def test_liquidity_full(self):
        score = score_liquidity(Decimal("1000"), Decimal("600"))
        assert score == Decimal("1.0")

    def test_liquidity_zero(self):
        score = score_liquidity(Decimal("50"), Decimal("50"))
        assert score == Decimal("0.0")

    def test_liquidity_partial(self):
        score = score_liquidity(Decimal("300"), Decimal("300"))
        assert Decimal("0") < score < Decimal("1.0")

    def test_liquidity_uses_smaller_side(self):
        score_a = score_liquidity(Decimal("1000"), Decimal("200"))
        score_b = score_liquidity(Decimal("200"), Decimal("1000"))
        assert score_a == score_b  # symmetric

    def test_volume_full(self):
        score = score_volume(Decimal("1000000"), Decimal("600000"))
        assert score == Decimal("1.0")

    def test_volume_unknown(self):
        score = score_volume(None, Decimal("1000000"))
        assert score == Decimal("0.5")

    def test_volume_both_unknown(self):
        score = score_volume(None, None)
        assert score == Decimal("0.5")

    def test_volume_low(self):
        score = score_volume(Decimal("50000"), Decimal("50000"))
        assert score == Decimal("0.0")

    def test_spread_magnitude_full(self):
        score = score_spread_magnitude(Decimal("25"))
        assert score == Decimal("1.0")

    def test_spread_magnitude_zero(self):
        score = score_spread_magnitude(Decimal("3"))
        assert score == Decimal("0.0")

    def test_spread_magnitude_partial(self):
        score = score_spread_magnitude(Decimal("12.5"))
        assert Decimal("0") < score < Decimal("1.0")

    def test_overall_confidence_perfect(self):
        """All factors at max → confidence = 1.0"""
        snap = make_snap(
            age_ms=100,
            ask_size="1000",
            bid_size="1000",
            volume_24h=Decimal("2000000"),
        )
        score = calculate_confidence(
            snap, snap,
            buy_ask_size=Decimal("1000"),
            sell_bid_size=Decimal("1000"),
            gross_spread_bps=Decimal("30"),
            now=NOW,
        )
        assert score == Decimal("1.0")

    def test_overall_confidence_poor(self):
        """All factors at min → confidence = 0.0 or near 0."""
        snap = make_snap(
            age_ms=5000,
            ask_size="10",
            bid_size="10",
            volume_24h=Decimal("1000"),
        )
        score = calculate_confidence(
            snap, snap,
            buy_ask_size=Decimal("10"),
            sell_bid_size=Decimal("10"),
            gross_spread_bps=Decimal("1"),
            now=NOW,
        )
        assert score == Decimal("0.0")

    def test_confidence_bounded(self):
        """Score should always be between 0 and 1."""
        snap = make_snap(age_ms=800)
        score = calculate_confidence(
            snap, snap,
            buy_ask_size=Decimal("300"),
            sell_bid_size=Decimal("300"),
            gross_spread_bps=Decimal("10"),
            now=NOW,
        )
        assert Decimal("0") <= score <= Decimal("1.0")
