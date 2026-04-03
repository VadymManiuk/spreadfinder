"""
Tests for market cap filter.

Covers: is_allowed logic, static data loading, filter_symbols,
always-include/exclude overrides, unknown market cap handling.
"""

from decimal import Decimal

import pytest

from filters.market_cap_filter import MarketCapFilter, HARD_EXCLUDE_MCAP


class TestIsAllowed:

    def setup_method(self):
        self.f = MarketCapFilter(max_mcap=200_000_000, min_mcap=0)
        self.f.load_static({
            "APE": 150_000_000,      # $150M — within range
            "DOGE": 80_000_000,      # $80M — within range
            "PEPE": 50_000_000,      # $50M — within range
            "LINK": 500_000_000,     # $500M — above max
            "BTC": 1_200_000_000_000, # $1.2T — way above hard ceiling
            "ETH": 400_000_000_000,  # $400B — above hard ceiling
            "SOL": 800_000_000,      # $800M — above max but below $1B
        })

    def test_small_cap_allowed(self):
        assert self.f.is_allowed("APE") is True
        assert self.f.is_allowed("DOGE") is True
        assert self.f.is_allowed("PEPE") is True

    def test_above_max_rejected(self):
        assert self.f.is_allowed("LINK") is False
        assert self.f.is_allowed("SOL") is False

    def test_above_hard_ceiling_rejected(self):
        assert self.f.is_allowed("BTC") is False
        assert self.f.is_allowed("ETH") is False

    def test_unknown_mcap_included(self):
        """Unknown tokens should be included — don't miss opportunities."""
        assert self.f.is_allowed("NEWTOKEN") is True

    def test_case_insensitive(self):
        assert self.f.is_allowed("ape") is True
        assert self.f.is_allowed("Ape") is True

    def test_exact_boundary_max(self):
        self.f.load_static({"EDGE": 200_000_000})
        assert self.f.is_allowed("EDGE") is True  # equal to max, not above

    def test_just_above_max(self):
        self.f.load_static({"EDGE": 200_000_001})
        assert self.f.is_allowed("EDGE") is False


class TestAlwaysOverrides:

    def setup_method(self):
        self.f = MarketCapFilter(max_mcap=200_000_000)
        self.f.load_static({
            "BTC": 1_200_000_000_000,
            "APE": 150_000_000,
        })

    def test_always_include_overrides_mcap(self):
        """Force-include a large cap for testing purposes."""
        self.f.add_always_include(["BTC"])
        assert self.f.is_allowed("BTC") is True

    def test_always_exclude_overrides_mcap(self):
        """Force-exclude a small cap."""
        self.f.add_always_exclude(["APE"])
        assert self.f.is_allowed("APE") is False

    def test_always_include_beats_always_exclude(self):
        """Include takes priority over exclude."""
        self.f.add_always_include(["APE"])
        self.f.add_always_exclude(["APE"])
        assert self.f.is_allowed("APE") is True


class TestFilterSymbols:

    def test_filters_list(self):
        f = MarketCapFilter(max_mcap=200_000_000)
        f.load_static({
            "APE": 150_000_000,
            "LINK": 500_000_000,
            "PEPE": 50_000_000,
            "BTC": 1_200_000_000_000,
        })
        result = f.filter_symbols(["APE", "LINK", "PEPE", "BTC", "UNKNOWN"])
        assert "APE" in result
        assert "PEPE" in result
        assert "UNKNOWN" in result  # unknown included
        assert "LINK" not in result
        assert "BTC" not in result


class TestGetMcap:

    def test_returns_value(self):
        f = MarketCapFilter()
        f.load_static({"APE": 150_000_000})
        assert f.get_mcap("APE") == 150_000_000

    def test_returns_none_for_unknown(self):
        f = MarketCapFilter()
        assert f.get_mcap("UNKNOWN") is None
