"""
Tests for ticker alias normalization.

Covers: normalize_base, are_same_asset, get_aliases,
1000x token detection, rebranded tokens.
"""

import pytest

from symbol_mapper.ticker_aliases import (
    normalize_base,
    are_same_asset,
    get_aliases,
)


class TestNormalizeBase:

    def test_no_alias_passthrough(self):
        assert normalize_base("BTC") == "BTC"
        assert normalize_base("ETH") == "ETH"
        assert normalize_base("APE") == "APE"

    def test_1000x_prefix(self):
        assert normalize_base("1000PEPE") == "PEPE"
        assert normalize_base("1000FLOKI") == "FLOKI"
        assert normalize_base("1000SHIB") == "SHIB"
        assert normalize_base("1000BONK") == "BONK"
        assert normalize_base("1000SATS") == "SATS"

    def test_rebrand_matic_pol(self):
        assert normalize_base("MATIC") == "POL"

    def test_terra_classic(self):
        assert normalize_base("LUNA") == "LUNC"

    def test_asi_alliance_mergers(self):
        # AGIX and OCEAN merged into FET
        assert normalize_base("AGIX") == "FET"
        assert normalize_base("OCEAN") == "FET"

    def test_case_insensitive(self):
        assert normalize_base("matic") == "POL"
        assert normalize_base("1000pepe") == "PEPE"

    def test_hyperliquid_k_prefix(self):
        assert normalize_base("kPEPE") == "PEPE"
        assert normalize_base("kFLOKI") == "FLOKI"
        assert normalize_base("kBONK") == "BONK"


class TestAreSameAsset:

    def test_same_ticker(self):
        assert are_same_asset("BTC", "BTC") is True

    def test_1000x_match(self):
        assert are_same_asset("1000PEPE", "PEPE") is True
        assert are_same_asset("PEPE", "1000PEPE") is True

    def test_rebrand_match(self):
        assert are_same_asset("MATIC", "POL") is True
        assert are_same_asset("POL", "MATIC") is True

    def test_different_assets(self):
        assert are_same_asset("BTC", "ETH") is False
        assert are_same_asset("PEPE", "DOGE") is False

    def test_hyperliquid_k_prefix_match(self):
        assert are_same_asset("kPEPE", "1000PEPE") is True
        assert are_same_asset("kPEPE", "PEPE") is True


class TestGetAliases:

    def test_pepe_aliases(self):
        aliases = get_aliases("PEPE")
        assert "1000PEPE" in aliases
        assert "KPEPE" in aliases
        assert "PEPE" in aliases

    def test_unknown_token(self):
        aliases = get_aliases("UNKNOWN_TOKEN_XYZ")
        assert aliases == {"UNKNOWN_TOKEN_XYZ"}
