"""
Tests for opportunity filters and filter chain.

Covers: threshold filters (boundary values), cooldown logic,
persistence logic, filter chain short-circuiting.
"""

import time
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import pytest

from models.snapshot import SpreadOpportunity
from filters.opportunity_filters import (
    check_min_gross_spread,
    check_max_gross_spread,
    check_min_net_spread,
    check_min_bid_size,
    check_min_ask_size,
    check_min_volume,
    check_max_data_age,
    check_min_confidence,
    CooldownFilter,
    PersistenceFilter,
)
from filters.filter_chain import FilterChain


NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def make_opp(**overrides) -> SpreadOpportunity:
    """Helper to create a SpreadOpportunity with passing defaults."""
    defaults = {
        "canonical_symbol": "APE-USDT-PERP",
        "buy_exchange": "binance",
        "sell_exchange": "gate",
        "buy_ask": Decimal("1.2000"),
        "sell_bid": Decimal("1.2100"),
        "gross_spread": Decimal("0.0100"),
        "gross_spread_bps": Decimal("83.33"),
        "net_spread": Decimal("0.0050"),
        "net_spread_bps": Decimal("41.67"),
        "estimated_fees": Decimal("0.0040"),
        "estimated_slippage": Decimal("0.0010"),
        "buy_ask_size": Decimal("500"),
        "sell_bid_size": Decimal("400"),
        "buy_volume_24h": Decimal("2000000"),
        "sell_volume_24h": Decimal("1500000"),
        "data_age_ms": 200,
        "confidence": Decimal("0.85"),
        "timestamp": NOW,
    }
    defaults.update(overrides)
    return SpreadOpportunity(**defaults)


# ---------------------------------------------------------------------------
# Individual filter tests — boundary values
# ---------------------------------------------------------------------------

class TestMinGrossSpread:
    def test_pass(self):
        assert check_min_gross_spread(make_opp(gross_spread_bps=Decimal("15")), Decimal("10"))

    def test_exact_boundary(self):
        assert check_min_gross_spread(make_opp(gross_spread_bps=Decimal("10")), Decimal("10"))

    def test_reject(self):
        result = check_min_gross_spread(make_opp(gross_spread_bps=Decimal("9.9")), Decimal("10"))
        assert not result
        assert "gross_spread_bps" in result.reason


class TestMinNetSpread:
    def test_pass(self):
        assert check_min_net_spread(make_opp(net_spread_bps=Decimal("10")), Decimal("5"))

    def test_reject(self):
        assert not check_min_net_spread(make_opp(net_spread_bps=Decimal("4")), Decimal("5"))

    def test_negative_net(self):
        assert not check_min_net_spread(make_opp(net_spread_bps=Decimal("-2")), Decimal("0"))


class TestMaxGrossSpread:
    def test_pass(self):
        assert check_max_gross_spread(make_opp(gross_spread_bps=Decimal("499.9")), Decimal("500"))

    def test_exact_boundary(self):
        assert check_max_gross_spread(make_opp(gross_spread_bps=Decimal("500")), Decimal("500"))

    def test_reject(self):
        result = check_max_gross_spread(make_opp(gross_spread_bps=Decimal("500.1")), Decimal("500"))
        assert not result
        assert "gross_spread_bps" in result.reason


class TestMinBidSize:
    def test_pass(self):
        assert check_min_bid_size(make_opp(sell_bid_size=Decimal("200")), Decimal("100"))

    def test_reject(self):
        assert not check_min_bid_size(make_opp(sell_bid_size=Decimal("50")), Decimal("100"))


class TestMinAskSize:
    def test_pass(self):
        assert check_min_ask_size(make_opp(buy_ask_size=Decimal("200")), Decimal("100"))

    def test_reject(self):
        assert not check_min_ask_size(make_opp(buy_ask_size=Decimal("50")), Decimal("100"))


class TestMinVolume:
    def test_pass(self):
        assert check_min_volume(make_opp(), Decimal("1000000"))

    def test_reject_buy_side(self):
        assert not check_min_volume(
            make_opp(buy_volume_24h=Decimal("500")), Decimal("1000")
        )

    def test_reject_sell_side(self):
        assert not check_min_volume(
            make_opp(sell_volume_24h=Decimal("500")), Decimal("1000")
        )

    def test_none_threshold_always_passes(self):
        assert check_min_volume(make_opp(), None)

    def test_unknown_volume_passes(self):
        """If volume is None on the opportunity, don't reject (no data to compare)."""
        assert check_min_volume(make_opp(buy_volume_24h=None), Decimal("1000"))


class TestMaxDataAge:
    def test_pass(self):
        assert check_max_data_age(make_opp(data_age_ms=100), 2000)

    def test_exact_boundary(self):
        assert check_max_data_age(make_opp(data_age_ms=2000), 2000)

    def test_reject(self):
        assert not check_max_data_age(make_opp(data_age_ms=2001), 2000)


class TestMinConfidence:
    def test_pass(self):
        assert check_min_confidence(make_opp(confidence=Decimal("0.8")), Decimal("0.3"))

    def test_reject(self):
        assert not check_min_confidence(make_opp(confidence=Decimal("0.1")), Decimal("0.3"))

    def test_exact_boundary(self):
        assert check_min_confidence(make_opp(confidence=Decimal("0.3")), Decimal("0.3"))


# ---------------------------------------------------------------------------
# Cooldown filter
# ---------------------------------------------------------------------------

class TestCooldownFilter:

    def test_first_check_passes(self):
        cf = CooldownFilter(cooldown_seconds=60)
        assert cf.check(make_opp())

    def test_reject_within_cooldown(self):
        cf = CooldownFilter(cooldown_seconds=60)
        opp = make_opp()
        cf.record_alert(opp)
        result = cf.check(opp)
        assert not result
        assert "cooldown" in result.reason

    def test_pass_after_cooldown_expires(self):
        cf = CooldownFilter(cooldown_seconds=1)
        opp = make_opp()
        cf.record_alert(opp)
        # Manually set last alert time to the past
        key = cf._make_key(opp)
        cf._last_alert[key] = time.monotonic() - 2
        assert cf.check(opp)

    def test_different_pairs_independent(self):
        cf = CooldownFilter(cooldown_seconds=60)
        opp_a = make_opp(buy_exchange="binance", sell_exchange="gate")
        opp_b = make_opp(buy_exchange="gate", sell_exchange="binance")
        cf.record_alert(opp_a)
        assert not cf.check(opp_a)
        assert cf.check(opp_b)  # different direction, should pass

    def test_different_symbols_independent(self):
        cf = CooldownFilter(cooldown_seconds=60)
        opp_a = make_opp(canonical_symbol="APE-USDT-PERP")
        opp_b = make_opp(canonical_symbol="XRP-USDT-PERP")
        cf.record_alert(opp_a)
        assert not cf.check(opp_a)
        assert cf.check(opp_b)

    def test_clear(self):
        cf = CooldownFilter(cooldown_seconds=60)
        opp = make_opp()
        cf.record_alert(opp)
        cf.clear()
        assert cf.check(opp)


# ---------------------------------------------------------------------------
# Persistence filter
# ---------------------------------------------------------------------------

class TestPersistenceFilter:

    def test_first_seen_rejects(self):
        pf = PersistenceFilter(persistence_ms=1000)
        result = pf.check(make_opp())
        assert not result
        assert "first seen" in result.reason

    def test_pass_after_persistence(self):
        pf = PersistenceFilter(persistence_ms=100)
        opp = make_opp()
        pf.check(opp)  # first seen
        # Manually set first_seen to the past
        key = pf._make_key(opp)
        pf._first_seen[key] = time.monotonic() * 1000 - 200
        assert pf.check(opp)

    def test_still_waiting(self):
        pf = PersistenceFilter(persistence_ms=5000)
        opp = make_opp()
        pf.check(opp)
        result = pf.check(opp)  # immediately after
        assert not result

    def test_remove_resets(self):
        pf = PersistenceFilter(persistence_ms=100)
        opp = make_opp()
        pf.check(opp)
        pf.remove(opp)
        result = pf.check(opp)
        assert not result  # should be "first seen" again

    def test_clear(self):
        pf = PersistenceFilter(persistence_ms=100)
        opp = make_opp()
        pf.check(opp)
        pf.clear()
        result = pf.check(opp)
        assert not result  # reset


# ---------------------------------------------------------------------------
# Filter chain
# ---------------------------------------------------------------------------

class TestFilterChain:

    def test_all_pass(self):
        chain = FilterChain(
            min_gross_spread_bps=Decimal("10"),
            min_net_spread_bps=Decimal("5"),
            min_bid_size=Decimal("100"),
            min_ask_size=Decimal("100"),
            max_data_age_ms=2000,
            min_confidence=Decimal("0.3"),
            cooldown_seconds=0,
            persistence_ms=0,
        )
        passed, results = chain.evaluate(make_opp())
        assert passed
        assert all(r.passed for r in results)

    def test_short_circuits_on_first_failure(self):
        chain = FilterChain(
            min_gross_spread_bps=Decimal("999"),  # will fail
            min_net_spread_bps=Decimal("5"),
            cooldown_seconds=0,
            persistence_ms=0,
        )
        passed, results = chain.evaluate(make_opp())
        assert not passed
        # Should have stopped at first filter
        assert len(results) == 1
        assert results[0].filter_name == "min_gross_spread"

    def test_configurable_max_gross_spread_can_allow_large_moves(self):
        chain = FilterChain(
            min_gross_spread_bps=Decimal("1"),
            max_gross_spread_bps=Decimal("2500"),
            min_net_spread_bps=Decimal("1"),
            min_bid_size=Decimal("1"),
            min_ask_size=Decimal("1"),
            max_data_age_ms=5000,
            min_confidence=Decimal("0.0"),
            cooldown_seconds=0,
            persistence_ms=0,
        )
        opp = make_opp(
            gross_spread_bps=Decimal("1967.92"),
            net_spread_bps=Decimal("1951.44"),
        )

        passed, results = chain.evaluate(opp)

        assert passed
        assert all(r.passed for r in results)

    def test_cooldown_blocks_second_alert(self):
        chain = FilterChain(
            min_gross_spread_bps=Decimal("1"),
            min_net_spread_bps=Decimal("1"),
            min_bid_size=Decimal("1"),
            min_ask_size=Decimal("1"),
            max_data_age_ms=5000,
            min_confidence=Decimal("0.0"),
            cooldown_seconds=60,
            persistence_ms=0,
        )
        opp = make_opp()
        passed1, _ = chain.evaluate(opp)
        assert passed1
        chain.record_alert(opp)
        passed2, results2 = chain.evaluate(opp)
        assert not passed2
        failed = [r for r in results2 if not r.passed]
        assert failed[0].filter_name == "cooldown"

    def test_clear_state_resets_everything(self):
        chain = FilterChain(
            min_gross_spread_bps=Decimal("1"),
            min_net_spread_bps=Decimal("1"),
            min_bid_size=Decimal("1"),
            min_ask_size=Decimal("1"),
            max_data_age_ms=5000,
            min_confidence=Decimal("0.0"),
            cooldown_seconds=60,
            persistence_ms=0,
        )
        opp = make_opp()
        chain.evaluate(opp)
        chain.record_alert(opp)
        chain.clear_state()
        passed, _ = chain.evaluate(opp)
        assert passed
