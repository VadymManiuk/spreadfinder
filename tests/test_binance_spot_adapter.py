"""
Tests for Binance spot adapter message parsing and snapshot emission.

Covers: bookTicker parsing, snapshot emission, and WS URL construction.
Uses raw JSON fixtures — no live WebSocket connection needed.
"""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from exchange_adapters.binance_spot import BinanceSpotAdapter
from models.snapshot import MarketSnapshot


BOOK_TICKER_RAW = {
    "u": 1,
    "s": "AIUSDT",
    "b": "0.1200",
    "B": "500.0",
    "a": "0.1210",
    "A": "750.0",
    "E": 1704067200000,
}


class TestBinanceSpotBookTickerParsing:

    def test_parse_basic(self):
        result = BinanceSpotAdapter.parse_book_ticker(BOOK_TICKER_RAW)
        assert result["symbol"] == "AIUSDT"
        assert result["bid"] == Decimal("0.1200")
        assert result["ask"] == Decimal("0.1210")
        assert result["bid_size"] == Decimal("500.0")
        assert result["ask_size"] == Decimal("750.0")

    def test_parse_exchange_ts(self):
        result = BinanceSpotAdapter.parse_book_ticker(BOOK_TICKER_RAW)
        assert result["exchange_ts"] == datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


class TestBinanceSpotSnapshotEmission:

    @pytest.mark.asyncio
    async def test_book_ticker_emits_spot_snapshot(self):
        snapshots = []

        async def collector(snap: MarketSnapshot):
            snapshots.append(snap)

        adapter = BinanceSpotAdapter(
            symbols=["AIUSDT"],
            on_snapshot=collector,
            canonical_map={"AIUSDT": "AI-USDT-SPOT"},
        )

        await adapter._handle_book_ticker(BOOK_TICKER_RAW)

        assert len(snapshots) == 1
        snap = snapshots[0]
        assert snap.canonical_symbol == "AI-USDT-SPOT"
        assert snap.exchange == "binance_spot"
        assert snap.bid == Decimal("0.1200")
        assert snap.ask == Decimal("0.1210")
        assert snap.funding_rate is None


class TestBinanceSpotWSUrl:

    def test_url_format(self):
        adapter = BinanceSpotAdapter(
            symbols=["AIUSDT"],
            on_snapshot=lambda s: None,
        )
        url = adapter._build_ws_url()
        assert "stream.binance.com:9443/stream?streams=" in url
        assert "aiusdt@bookTicker" in url
