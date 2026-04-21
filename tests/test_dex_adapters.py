"""
Tests for DEX polling adapters.

Covers: parsing public Binance Alpha payloads, parsing OKX DEX toplist items,
and filtering out non-matchable / ambiguous bases before snapshots are emitted.
"""

from decimal import Decimal

from exchange_adapters.binance_alpha import BinanceAlphaAdapter
from exchange_adapters.okx_dex import OkxDexAdapter


async def _noop(_snapshot):
    return None


def test_binance_alpha_builds_snapshot_from_live_shape():
    adapter = BinanceAlphaAdapter(
        allowed_bases={"OPG"},
        on_snapshot=_noop,
    )
    token = {
        "chainId": "56",
        "symbol": "OPG",
        "cexCoinName": "",
        "price": "0.37572098348266837646",
        "volume24h": "64312145.332817354623966955048",
        "liquidity": "1571082.1724615300373543",
        "offline": False,
        "fullyDelisted": False,
    }

    snapshot = adapter._build_snapshot(token)

    assert snapshot is not None
    assert snapshot.exchange == "binance_alpha:56"
    assert snapshot.canonical_symbol == "OPG-56-DEX"
    assert snapshot.bid == Decimal("0.37572098348266837646")
    assert snapshot.ask == Decimal("0.37572098348266837646")
    assert snapshot.volume_24h == Decimal("64312145.332817354623966955048")
    assert snapshot.ask_size > Decimal("0")


def test_binance_alpha_prefers_cex_coin_name_for_multiplier_symbols():
    adapter = BinanceAlphaAdapter(
        allowed_bases={"BONK"},
        on_snapshot=_noop,
    )
    token = {
        "chainId": "56",
        "symbol": "Bonk",
        "cexCoinName": "1000BONK",
        "price": "0.00003125",
        "volume24h": "1588891.92",
        "liquidity": "6583220.96",
        "offline": False,
        "fullyDelisted": False,
    }

    snapshot = adapter._build_snapshot(token)

    assert snapshot is not None
    assert snapshot.canonical_symbol == "1000BONK-56-DEX"


def test_okx_dex_builds_snapshot_from_toplist_item():
    adapter = OkxDexAdapter(
        allowed_bases={"OPG"},
        chain_indices=["8453"],
        api_key="k",
        api_secret="s",
        passphrase="p",
        project_id="",
        on_snapshot=_noop,
    )
    token = {
        "chainIndex": "8453",
        "tokenSymbol": "OPG",
        "price": "0.4021",
        "volume": "5123456.78",
        "liquidity": "2012345.67",
    }

    snapshot = adapter._build_snapshot(token)

    assert snapshot is not None
    assert snapshot.exchange == "okx_dex:8453"
    assert snapshot.canonical_symbol == "OPG-8453-DEX"
    assert snapshot.bid == Decimal("0.4021")
    assert snapshot.volume_24h == Decimal("5123456.78")
    assert snapshot.bid_size > Decimal("0")
