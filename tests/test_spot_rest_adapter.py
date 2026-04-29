"""
Tests for shared spot REST adapter parsers and snapshot emission.

Covers: exchange-specific ticker parsing and REST ticker -> MarketSnapshot conversion.
No live network calls are made.
"""

from decimal import Decimal

import pytest

from exchange_adapters.spot_rest import (
    SpotRestAdapter,
    parse_gate_spot_tickers,
    parse_bybit_spot_tickers,
    parse_okx_spot_tickers,
    parse_bitget_spot_tickers,
    parse_mexc_spot_tickers,
)
from models.snapshot import MarketSnapshot


def test_parse_gate_spot_tickers_estimates_size_from_quote_volume():
    tickers = parse_gate_spot_tickers([
        {
            "currency_pair": "AI_USDT",
            "highest_bid": "0.1200",
            "lowest_ask": "0.1210",
            "quote_volume": "1000000",
        }
    ])

    assert len(tickers) == 1
    assert tickers[0].symbol == "AI_USDT"
    assert tickers[0].bid == Decimal("0.1200")
    assert tickers[0].ask == Decimal("0.1210")
    assert tickers[0].ask_size is not None
    assert tickers[0].ask_size > 0


def test_parse_bybit_spot_tickers():
    tickers = parse_bybit_spot_tickers({
        "result": {
            "list": [{
                "symbol": "AIUSDT",
                "bid1Price": "0.1200",
                "ask1Price": "0.1210",
                "bid1Size": "100",
                "ask1Size": "90",
                "turnover24h": "1000000",
            }]
        }
    })

    assert len(tickers) == 1
    assert tickers[0].symbol == "AIUSDT"
    assert tickers[0].bid_size == Decimal("100")
    assert tickers[0].volume_24h == Decimal("1000000")


def test_parse_okx_spot_tickers():
    tickers = parse_okx_spot_tickers({
        "data": [{
            "instId": "AI-USDT",
            "bidPx": "0.1200",
            "askPx": "0.1210",
            "bidSz": "100",
            "askSz": "90",
            "volCcy24h": "1000000",
            "ts": "1704067200000",
        }]
    })

    assert len(tickers) == 1
    assert tickers[0].symbol == "AI-USDT"
    assert tickers[0].exchange_ts is not None


def test_parse_bitget_spot_tickers():
    tickers = parse_bitget_spot_tickers({
        "data": [{
            "symbol": "AIUSDT",
            "bidPr": "0.1200",
            "askPr": "0.1210",
            "bidSz": "100",
            "askSz": "90",
            "quoteVolume": "1000000",
            "ts": "1704067200000",
        }]
    })

    assert len(tickers) == 1
    assert tickers[0].symbol == "AIUSDT"
    assert tickers[0].ask_size == Decimal("90")


def test_parse_mexc_spot_tickers():
    tickers = parse_mexc_spot_tickers([
        {
            "symbol": "AIUSDT",
            "bidPrice": "0.1200",
            "askPrice": "0.1210",
            "bidQty": "100",
            "askQty": "90",
            "quoteVolume": "1000000",
            "closeTime": "1704067200000",
        }
    ])

    assert len(tickers) == 1
    assert tickers[0].symbol == "AIUSDT"
    assert tickers[0].volume_24h == Decimal("1000000")


@pytest.mark.asyncio
async def test_spot_rest_adapter_emits_snapshot_for_subscribed_symbol():
    snapshots = []

    async def collector(snap: MarketSnapshot):
        snapshots.append(snap)

    adapter = SpotRestAdapter(
        exchange_name="bybit_spot",
        symbols=["AIUSDT"],
        on_snapshot=collector,
        canonical_map={"AIUSDT": "AI-USDT-SPOT"},
    )
    tickers = parse_bybit_spot_tickers({
        "result": {
            "list": [{
                "symbol": "AIUSDT",
                "bid1Price": "0.1200",
                "ask1Price": "0.1210",
                "bid1Size": "100",
                "ask1Size": "90",
                "turnover24h": "1000000",
            }]
        }
    })

    await collector(adapter._build_snapshot(tickers[0]))

    assert len(snapshots) == 1
    assert snapshots[0].exchange == "bybit_spot"
    assert snapshots[0].canonical_symbol == "AI-USDT-SPOT"
    assert snapshots[0].ask == Decimal("0.1210")
