"""
Tests for normalized market data models.

Covers: validation, computed properties, immutability, edge cases.
"""

from datetime import datetime, timezone, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from models.snapshot import MarketSnapshot, SpreadOpportunity


# ---------------------------------------------------------------------------
# MarketSnapshot
# ---------------------------------------------------------------------------

def make_snapshot(**overrides) -> MarketSnapshot:
    """Helper to create a MarketSnapshot with sensible defaults."""
    defaults = {
        "canonical_symbol": "BTC-USDT-PERP",
        "exchange": "binance",
        "bid": Decimal("50000.00"),
        "ask": Decimal("50010.00"),
        "bid_size": Decimal("1.5"),
        "ask_size": Decimal("2.0"),
        "local_ts": datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    }
    defaults.update(overrides)
    return MarketSnapshot(**defaults)


class TestMarketSnapshot:

    def test_basic_creation(self):
        snap = make_snapshot()
        assert snap.canonical_symbol == "BTC-USDT-PERP"
        assert snap.exchange == "binance"
        assert snap.bid == Decimal("50000.00")
        assert snap.ask == Decimal("50010.00")
        assert snap.is_stale is False

    def test_optional_fields_default_none(self):
        snap = make_snapshot()
        assert snap.exchange_ts is None
        assert snap.mark_price is None
        assert snap.index_price is None
        assert snap.funding_rate is None
        assert snap.volume_24h is None

    def test_optional_fields_set(self):
        snap = make_snapshot(
            exchange_ts=datetime(2026, 1, 1, 11, 59, 59, tzinfo=timezone.utc),
            mark_price=Decimal("50005.00"),
            index_price=Decimal("50003.00"),
            funding_rate=Decimal("0.0001"),
            volume_24h=Decimal("1000000000"),
        )
        assert snap.mark_price == Decimal("50005.00")
        assert snap.funding_rate == Decimal("0.0001")

    def test_mid_price(self):
        snap = make_snapshot(bid=Decimal("100"), ask=Decimal("200"))
        # mid_price = (100 + 200) / 2 = 150
        assert snap.mid_price == Decimal("150")

    def test_spread_bps(self):
        snap = make_snapshot(bid=Decimal("99"), ask=Decimal("100"))
        # spread_bps = ((100 - 99) / 100) * 10000 = 100 bps
        assert snap.spread_bps == Decimal("100")

    def test_spread_bps_zero_ask(self):
        snap = make_snapshot(bid=Decimal("0"), ask=Decimal("0"))
        assert snap.spread_bps == Decimal("0")

    def test_data_age_ms(self):
        ts = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        snap = make_snapshot(local_ts=ts)
        now = ts + timedelta(milliseconds=500)
        assert snap.data_age_ms(now=now) == 500

    def test_data_age_ms_zero(self):
        ts = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        snap = make_snapshot(local_ts=ts)
        assert snap.data_age_ms(now=ts) == 0

    def test_immutability(self):
        snap = make_snapshot()
        with pytest.raises(ValidationError):
            snap.bid = Decimal("99999")  # type: ignore[misc]

    def test_stale_flag(self):
        snap = make_snapshot(is_stale=True)
        assert snap.is_stale is True

    def test_requires_canonical_symbol(self):
        with pytest.raises(ValidationError):
            MarketSnapshot(
                exchange="binance",
                bid=Decimal("100"),
                ask=Decimal("101"),
                bid_size=Decimal("1"),
                ask_size=Decimal("1"),
            )

    def test_requires_exchange(self):
        with pytest.raises(ValidationError):
            MarketSnapshot(
                canonical_symbol="BTC-USDT-PERP",
                bid=Decimal("100"),
                ask=Decimal("101"),
                bid_size=Decimal("1"),
                ask_size=Decimal("1"),
            )


# ---------------------------------------------------------------------------
# SpreadOpportunity
# ---------------------------------------------------------------------------

def make_opportunity(**overrides) -> SpreadOpportunity:
    """Helper to create a SpreadOpportunity with sensible defaults."""
    defaults = {
        "canonical_symbol": "BTC-USDT-PERP",
        "buy_exchange": "binance",
        "sell_exchange": "hyperliquid",
        "buy_ask": Decimal("50010.00"),
        "sell_bid": Decimal("50050.00"),
        "gross_spread": Decimal("40.00"),
        "gross_spread_bps": Decimal("7.998"),
        "net_spread": Decimal("20.00"),
        "net_spread_bps": Decimal("3.999"),
        "estimated_fees": Decimal("15.00"),
        "estimated_slippage": Decimal("5.00"),
        "buy_ask_size": Decimal("2.0"),
        "sell_bid_size": Decimal("1.5"),
        "data_age_ms": 150,
        "confidence": Decimal("0.85"),
        "timestamp": datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
    }
    defaults.update(overrides)
    return SpreadOpportunity(**defaults)


class TestSpreadOpportunity:

    def test_basic_creation(self):
        opp = make_opportunity()
        assert opp.canonical_symbol == "BTC-USDT-PERP"
        assert opp.buy_exchange == "binance"
        assert opp.sell_exchange == "hyperliquid"
        assert opp.gross_spread == Decimal("40.00")
        assert opp.net_spread == Decimal("20.00")

    def test_optional_funding_rates(self):
        opp = make_opportunity()
        assert opp.buy_funding_rate is None
        assert opp.sell_funding_rate is None

    def test_funding_rates_set(self):
        opp = make_opportunity(
            buy_funding_rate=Decimal("0.0001"),
            sell_funding_rate=Decimal("-0.0002"),
        )
        assert opp.buy_funding_rate == Decimal("0.0001")
        assert opp.sell_funding_rate == Decimal("-0.0002")

    def test_optional_volumes(self):
        opp = make_opportunity()
        assert opp.buy_volume_24h is None
        assert opp.sell_volume_24h is None

    def test_volumes_set(self):
        opp = make_opportunity(
            buy_volume_24h=Decimal("500000000"),
            sell_volume_24h=Decimal("300000000"),
        )
        assert opp.buy_volume_24h == Decimal("500000000")

    def test_immutability(self):
        opp = make_opportunity()
        with pytest.raises(ValidationError):
            opp.gross_spread = Decimal("999")  # type: ignore[misc]

    def test_confidence_range(self):
        # Model doesn't enforce range — that's the confidence scorer's job.
        # But it should accept boundary values.
        opp_low = make_opportunity(confidence=Decimal("0.0"))
        opp_high = make_opportunity(confidence=Decimal("1.0"))
        assert opp_low.confidence == Decimal("0.0")
        assert opp_high.confidence == Decimal("1.0")

    def test_negative_net_spread(self):
        """Net spread can be negative when costs exceed gross spread."""
        opp = make_opportunity(
            gross_spread=Decimal("10.00"),
            net_spread=Decimal("-5.00"),
            net_spread_bps=Decimal("-1.0"),
        )
        assert opp.net_spread < 0
