"""
Tests for market cap filter.

Covers: is_allowed logic, static data loading, filter_symbols,
always-include/exclude overrides, unknown market cap handling.
"""

from decimal import Decimal

import pytest

from filters.market_cap_filter import MarketCapFilter, HARD_EXCLUDE_MCAP


class FakeResponse:
    def __init__(self, payload, status: int = 200):
        self.payload = payload
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self):
        if self.status >= 400:
            raise AssertionError(f"unexpected status {self.status}")

    async def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses: list[FakeResponse]):
        self.responses = responses
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


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


class TestOnDemandLookup:

    @pytest.mark.asyncio
    async def test_uses_coinmarketcap_when_key_is_configured(self):
        session = FakeSession([
            FakeResponse({
                "data": {
                    "ESPORTS": [
                        {"quote": {"USD": {"market_cap": 12_345_678}}},
                    ],
                },
            }),
        ])
        f = MarketCapFilter(coinmarketcap_api_key="cmc-key")
        f._session = session

        assert await f.get_mcap_async("ESPORTS") == 12_345_678
        assert f.get_mcap("ESPORTS") == 12_345_678
        assert "coinmarketcap" in session.calls[0][0]

    @pytest.mark.asyncio
    async def test_falls_back_to_coingecko_search_for_long_tail_symbol(self):
        session = FakeSession([
            FakeResponse({
                "coins": [
                    {"id": "esports-token", "symbol": "esports", "market_cap_rank": 1800},
                    {"id": "not-esports", "symbol": "nope", "market_cap_rank": 10},
                ],
            }),
            FakeResponse([
                {"id": "esports-token", "symbol": "esports", "market_cap": 9_876_543},
            ]),
        ])
        f = MarketCapFilter()
        f._session = session

        assert await f.get_mcap_async("esports") == 9_876_543
        assert f.get_mcap("ESPORTS") == 9_876_543
        assert session.calls[0][1]["params"]["query"] == "ESPORTS"
        assert session.calls[1][1]["params"]["ids"] == "esports-token"
