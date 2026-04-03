"""
Tests for cross-exchange symbol normalization.

Covers: native↔canonical round-trips, edge cases, false match prevention,
common symbol detection, and static loading.
"""

import pytest

from symbol_mapper.exchange_symbols import (
    binance_native_to_canonical,
    binance_canonical_to_native,
    hyperliquid_native_to_canonical,
    hyperliquid_canonical_to_native,
    gate_native_to_canonical,
    gate_canonical_to_native,
)
from symbol_mapper.mapper import SymbolMapper


# ---------------------------------------------------------------------------
# Binance conversion functions
# ---------------------------------------------------------------------------

class TestBinanceConversion:

    def test_btcusdt(self):
        assert binance_native_to_canonical("BTCUSDT") == "BTC-USDT-PERP"

    def test_ethusdt(self):
        assert binance_native_to_canonical("ETHUSDT") == "ETH-USDT-PERP"

    def test_usdc_quote(self):
        assert binance_native_to_canonical("BTCUSDC") == "BTC-USDC-PERP"

    def test_busd_quote(self):
        assert binance_native_to_canonical("ETHBUSD") == "ETH-BUSD-PERP"

    def test_unknown_quote_returns_none(self):
        assert binance_native_to_canonical("BTCEUR") is None

    def test_empty_base_returns_none(self):
        assert binance_native_to_canonical("USDT") is None

    def test_empty_string_returns_none(self):
        assert binance_native_to_canonical("") is None

    def test_round_trip(self):
        canonical = binance_native_to_canonical("SOLUSDT")
        assert canonical == "SOL-USDT-PERP"
        native = binance_canonical_to_native(canonical)
        assert native == "SOLUSDT"

    def test_canonical_to_native_invalid(self):
        assert binance_canonical_to_native("BTC-USDT") is None
        assert binance_canonical_to_native("BTC-USDT-SPOT") is None


# ---------------------------------------------------------------------------
# Hyperliquid conversion functions
# ---------------------------------------------------------------------------

class TestHyperliquidConversion:

    def test_btc(self):
        assert hyperliquid_native_to_canonical("BTC") == "BTC-USDC-PERP"

    def test_eth(self):
        assert hyperliquid_native_to_canonical("ETH") == "ETH-USDC-PERP"

    def test_empty_returns_none(self):
        assert hyperliquid_native_to_canonical("") is None

    def test_dash_in_name_returns_none(self):
        # Prevent false matches with non-perpetual symbols
        assert hyperliquid_native_to_canonical("BTC-PERP") is None

    def test_round_trip(self):
        canonical = hyperliquid_native_to_canonical("SOL")
        assert canonical == "SOL-USDC-PERP"
        native = hyperliquid_canonical_to_native(canonical)
        assert native == "SOL"

    def test_canonical_wrong_quote_returns_none(self):
        # Hyperliquid only uses USDC
        assert hyperliquid_canonical_to_native("BTC-USDT-PERP") is None

    def test_canonical_invalid_format(self):
        assert hyperliquid_canonical_to_native("BTCUSDC") is None


# ---------------------------------------------------------------------------
# Gate conversion functions
# ---------------------------------------------------------------------------

class TestGateConversion:

    def test_btc_usdt(self):
        assert gate_native_to_canonical("BTC_USDT") == "BTC-USDT-PERP"

    def test_eth_usdt(self):
        assert gate_native_to_canonical("ETH_USDT") == "ETH-USDT-PERP"

    def test_no_underscore_returns_none(self):
        assert gate_native_to_canonical("BTCUSDT") is None

    def test_multiple_underscores_returns_none(self):
        assert gate_native_to_canonical("BTC_USDT_PERP") is None

    def test_empty_parts_returns_none(self):
        assert gate_native_to_canonical("_USDT") is None
        assert gate_native_to_canonical("BTC_") is None

    def test_round_trip(self):
        canonical = gate_native_to_canonical("SOL_USDT")
        assert canonical == "SOL-USDT-PERP"
        native = gate_canonical_to_native(canonical)
        assert native == "SOL_USDT"

    def test_canonical_invalid(self):
        assert gate_canonical_to_native("BTC-USDT") is None


# ---------------------------------------------------------------------------
# SymbolMapper with static data
# ---------------------------------------------------------------------------

class TestSymbolMapper:

    def setup_method(self):
        self.mapper = SymbolMapper(exchanges=["binance", "hyperliquid", "gate"])
        self.mapper.load_static("binance", {
            "BTCUSDT": "BTC-USDT-PERP",
            "ETHUSDT": "ETH-USDT-PERP",
            "SOLUSDT": "SOL-USDT-PERP",
        })
        self.mapper.load_static("hyperliquid", {
            "BTC": "BTC-USDC-PERP",
            "ETH": "ETH-USDC-PERP",
            "SOL": "SOL-USDC-PERP",
        })
        self.mapper.load_static("gate", {
            "BTC_USDT": "BTC-USDT-PERP",
            "ETH_USDT": "ETH-USDT-PERP",
        })

    def test_to_canonical(self):
        assert self.mapper.to_canonical("binance", "BTCUSDT") == "BTC-USDT-PERP"
        assert self.mapper.to_canonical("hyperliquid", "ETH") == "ETH-USDC-PERP"
        assert self.mapper.to_canonical("gate", "BTC_USDT") == "BTC-USDT-PERP"

    def test_to_canonical_unknown_returns_none(self):
        assert self.mapper.to_canonical("binance", "XYZUSDT") is None
        assert self.mapper.to_canonical("unknown_exchange", "BTC") is None

    def test_to_native(self):
        assert self.mapper.to_native("binance", "ETH-USDT-PERP") == "ETHUSDT"
        assert self.mapper.to_native("gate", "BTC-USDT-PERP") == "BTC_USDT"

    def test_to_native_unavailable_returns_none(self):
        # SOL is on binance and hyperliquid but not gate
        assert self.mapper.to_native("gate", "SOL-USDT-PERP") is None

    def test_get_exchange_symbols(self):
        binance_syms = self.mapper.get_exchange_symbols("binance")
        assert "BTC-USDT-PERP" in binance_syms
        assert "SOL-USDT-PERP" in binance_syms
        assert len(binance_syms) == 3

    def test_get_common_symbols_all_exchanges(self):
        # No symbol is common to ALL three because Hyperliquid uses USDC quote
        # and Binance/Gate use USDT quote — different canonical symbols.
        common = self.mapper.get_common_symbols()
        assert len(common) == 0

    def test_get_common_symbols_binance_gate(self):
        common = self.mapper.get_common_symbols(["binance", "gate"])
        assert "BTC-USDT-PERP" in common
        assert "ETH-USDT-PERP" in common
        # SOL is only on binance, not gate
        assert "SOL-USDT-PERP" not in common

    def test_get_common_symbols_empty_exchange(self):
        common = self.mapper.get_common_symbols(["nonexistent"])
        assert len(common) == 0

    def test_get_pairwise_common(self):
        pairs = self.mapper.get_pairwise_common()
        # binance-gate should have BTC and ETH
        bg = pairs.get(("binance", "gate"))
        assert bg is not None
        assert "BTC-USDT-PERP" in bg
        assert "ETH-USDT-PERP" in bg

    def test_pairwise_hyperliquid_binance(self):
        # Different quote assets so no overlap
        pairs = self.mapper.get_pairwise_common()
        bh = pairs.get(("binance", "hyperliquid"))
        assert bh is None  # no common symbols, so key won't exist

    def test_false_match_prevention(self):
        """Ensure we don't incorrectly match symbols across quote currencies."""
        # BTC-USDT-PERP (binance) != BTC-USDC-PERP (hyperliquid)
        assert self.mapper.to_canonical("binance", "BTCUSDT") != \
               self.mapper.to_canonical("hyperliquid", "BTC")


# ---------------------------------------------------------------------------
# Quote-equivalence and matchable pairs
# ---------------------------------------------------------------------------

class TestQuoteEquivalence:

    def test_extract_base(self):
        assert SymbolMapper.extract_base("APE-USDT-PERP") == "APE"
        assert SymbolMapper.extract_base("BTC-USDC-PERP") == "BTC"

    def test_extract_base_invalid(self):
        assert SymbolMapper.extract_base("BTCUSDT") is None
        assert SymbolMapper.extract_base("APE-USDT") is None

    def test_extract_quote(self):
        assert SymbolMapper.extract_quote("APE-USDT-PERP") == "USDT"
        assert SymbolMapper.extract_quote("BTC-USDC-PERP") == "USDC"

    def test_quotes_equivalent_same(self):
        assert SymbolMapper.are_quotes_equivalent("USDT", "USDT") is True

    def test_quotes_equivalent_cross(self):
        assert SymbolMapper.are_quotes_equivalent("USDT", "USDC") is True
        assert SymbolMapper.are_quotes_equivalent("USDC", "USDT") is True

    def test_quotes_not_equivalent(self):
        assert SymbolMapper.are_quotes_equivalent("USDT", "EUR") is False


class TestMatchablePairs:

    def setup_method(self):
        self.mapper = SymbolMapper(exchanges=["binance", "hyperliquid", "gate"])
        self.mapper.load_static("binance", {
            "BTCUSDT": "BTC-USDT-PERP",
            "ETHUSDT": "ETH-USDT-PERP",
            "APEUSDT": "APE-USDT-PERP",
        })
        self.mapper.load_static("hyperliquid", {
            "BTC": "BTC-USDC-PERP",
            "ETH": "ETH-USDC-PERP",
            "APE": "APE-USDC-PERP",
        })
        self.mapper.load_static("gate", {
            "BTC_USDT": "BTC-USDT-PERP",
            "ETH_USDT": "ETH-USDT-PERP",
        })

    def test_finds_cross_quote_pairs(self):
        """Binance USDT vs Hyperliquid USDC should match on same base."""
        pairs = self.mapper.get_matchable_pairs()
        bases = {p["base"] for p in pairs}
        assert "BTC" in bases
        assert "ETH" in bases
        assert "APE" in bases

    def test_binance_hyperliquid_matched(self):
        pairs = self.mapper.get_matchable_pairs()
        bh_pairs = [p for p in pairs
                     if {p["exchange_a"], p["exchange_b"]} == {"binance", "hyperliquid"}]
        assert len(bh_pairs) == 3  # BTC, ETH, APE

    def test_same_quote_also_matched(self):
        """Binance USDT vs Gate USDT (same quote) should also be matched."""
        pairs = self.mapper.get_matchable_pairs()
        bg_pairs = [p for p in pairs
                     if {p["exchange_a"], p["exchange_b"]} == {"binance", "gate"}]
        assert len(bg_pairs) == 2  # BTC, ETH

    def test_pair_structure(self):
        pairs = self.mapper.get_matchable_pairs()
        for p in pairs:
            assert "base" in p
            assert "exchange_a" in p
            assert "canonical_a" in p
            assert "exchange_b" in p
            assert "canonical_b" in p

    def test_no_false_base_matches(self):
        """Different base assets should never be matched."""
        pairs = self.mapper.get_matchable_pairs()
        for p in pairs:
            base_a = SymbolMapper.extract_base(p["canonical_a"])
            base_b = SymbolMapper.extract_base(p["canonical_b"])
            assert base_a == base_b
