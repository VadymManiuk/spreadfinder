"""
Tests for Binance adapter message parsing and snapshot emission.

Covers: bookTicker parsing, markPrice parsing, snapshot emission,
state merging, WS URL construction, stale detection.
Uses raw JSON fixtures — no live WebSocket connection needed.
"""

import asyncio
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import pytest

from exchange_adapters.binance import BinanceAdapter, MAX_STREAMS_PER_CONN
from models.snapshot import MarketSnapshot
from utils.reconnect import ExponentialBackoff


# ---------------------------------------------------------------------------
# Fixtures — raw Binance payloads
# ---------------------------------------------------------------------------

BOOK_TICKER_RAW = {
    "s": "APEUSDT",
    "b": "1.2340",
    "B": "500.0",
    "a": "1.2350",
    "A": "750.0",
    "T": 1704067200000,  # 2024-01-01 00:00:00 UTC
    "E": 1704067200001,
}

MARK_PRICE_RAW = {
    "s": "APEUSDT",
    "p": "1.2345",
    "i": "1.2343",
    "r": "0.00010000",
    "T": 1704067200000,
    "E": 1704067200001,
}

BOOK_TICKER_RAW_2 = {
    "s": "XRPUSDT",
    "b": "0.6200",
    "B": "10000.0",
    "a": "0.6210",
    "A": "8000.0",
    "T": 1704067200000,
}


# ---------------------------------------------------------------------------
# Static parsing tests (no connection needed)
# ---------------------------------------------------------------------------

class TestBookTickerParsing:

    def test_parse_basic(self):
        result = BinanceAdapter.parse_book_ticker(BOOK_TICKER_RAW)
        assert result["symbol"] == "APEUSDT"
        assert result["bid"] == Decimal("1.2340")
        assert result["ask"] == Decimal("1.2350")
        assert result["bid_size"] == Decimal("500.0")
        assert result["ask_size"] == Decimal("750.0")

    def test_parse_exchange_ts(self):
        result = BinanceAdapter.parse_book_ticker(BOOK_TICKER_RAW)
        expected = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert result["exchange_ts"] == expected

    def test_parse_no_timestamp(self):
        data = {"s": "APEUSDT", "b": "1.00", "a": "1.01", "B": "10", "A": "10"}
        result = BinanceAdapter.parse_book_ticker(data)
        assert "exchange_ts" not in result

    def test_parse_precision(self):
        data = {
            "s": "SHIBUSDT",
            "b": "0.00002345",
            "a": "0.00002346",
            "B": "1000000000",
            "A": "500000000",
        }
        result = BinanceAdapter.parse_book_ticker(data)
        assert result["bid"] == Decimal("0.00002345")
        assert result["bid_size"] == Decimal("1000000000")


class TestMarkPriceParsing:

    def test_parse_basic(self):
        result = BinanceAdapter.parse_mark_price(MARK_PRICE_RAW)
        assert result["symbol"] == "APEUSDT"
        assert result["mark_price"] == Decimal("1.2345")
        assert result["index_price"] == Decimal("1.2343")
        assert result["funding_rate"] == Decimal("0.00010000")

    def test_parse_missing_fields(self):
        data = {"s": "APEUSDT"}
        result = BinanceAdapter.parse_mark_price(data)
        assert result["mark_price"] == Decimal("0")
        assert result["index_price"] == Decimal("0")
        assert result["funding_rate"] == Decimal("0")


# ---------------------------------------------------------------------------
# Snapshot emission tests (async, mocked callback)
# ---------------------------------------------------------------------------

class TestSnapshotEmission:

    @pytest.fixture
    def collected_snapshots(self):
        return []

    @pytest.fixture
    def adapter(self, collected_snapshots):
        async def collector(snap: MarketSnapshot):
            collected_snapshots.append(snap)

        return BinanceAdapter(
            symbols=["APEUSDT", "XRPUSDT"],
            on_snapshot=collector,
            canonical_map={
                "APEUSDT": "APE-USDT-PERP",
                "XRPUSDT": "XRP-USDT-PERP",
            },
        )

    @pytest.mark.asyncio
    async def test_book_ticker_emits_snapshot(self, adapter, collected_snapshots):
        await adapter._handle_book_ticker(BOOK_TICKER_RAW)
        assert len(collected_snapshots) == 1
        snap = collected_snapshots[0]
        assert snap.canonical_symbol == "APE-USDT-PERP"
        assert snap.exchange == "binance"
        assert snap.bid == Decimal("1.2340")
        assert snap.ask == Decimal("1.2350")

    @pytest.mark.asyncio
    async def test_mark_price_alone_no_emit(self, adapter, collected_snapshots):
        """markPrice alone shouldn't emit — we need bid/ask from bookTicker first."""
        await adapter._handle_mark_price(MARK_PRICE_RAW)
        assert len(collected_snapshots) == 0

    @pytest.mark.asyncio
    async def test_mark_price_then_book_ticker_merges(self, adapter, collected_snapshots):
        """markPrice data should be merged into the snapshot once bookTicker arrives."""
        await adapter._handle_mark_price(MARK_PRICE_RAW)
        await adapter._handle_book_ticker(BOOK_TICKER_RAW)
        assert len(collected_snapshots) == 1
        snap = collected_snapshots[0]
        assert snap.mark_price == Decimal("1.2345")
        assert snap.index_price == Decimal("1.2343")
        assert snap.funding_rate == Decimal("0.00010000")

    @pytest.mark.asyncio
    async def test_multiple_symbols_isolated(self, adapter, collected_snapshots):
        """Different symbols should not interfere with each other."""
        await adapter._handle_book_ticker(BOOK_TICKER_RAW)
        await adapter._handle_book_ticker(BOOK_TICKER_RAW_2)
        assert len(collected_snapshots) == 2
        assert collected_snapshots[0].canonical_symbol == "APE-USDT-PERP"
        assert collected_snapshots[1].canonical_symbol == "XRP-USDT-PERP"

    @pytest.mark.asyncio
    async def test_snapshot_not_stale(self, adapter, collected_snapshots):
        await adapter._handle_book_ticker(BOOK_TICKER_RAW)
        assert collected_snapshots[0].is_stale is False

    @pytest.mark.asyncio
    async def test_snapshot_has_exchange_ts(self, adapter, collected_snapshots):
        await adapter._handle_book_ticker(BOOK_TICKER_RAW)
        expected_ts = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert collected_snapshots[0].exchange_ts == expected_ts

    @pytest.mark.asyncio
    async def test_no_canonical_map_uses_native(self):
        """Without canonical_map, native symbol is used as canonical_symbol."""
        snaps = []

        async def collector(snap):
            snaps.append(snap)

        adapter = BinanceAdapter(
            symbols=["APEUSDT"],
            on_snapshot=collector,
            canonical_map={},
        )
        await adapter._handle_book_ticker(BOOK_TICKER_RAW)
        assert snaps[0].canonical_symbol == "APEUSDT"


# ---------------------------------------------------------------------------
# WebSocket URL construction
# ---------------------------------------------------------------------------

class TestWSUrl:

    def test_url_format(self):
        adapter = BinanceAdapter(
            symbols=["APEUSDT"],
            on_snapshot=lambda s: None,
        )
        url = adapter._build_ws_url()
        assert "fstream.binance.com/stream?streams=" in url
        assert "apeusdt@bookTicker" in url
        assert "apeusdt@markPrice" in url

    def test_multiple_symbols(self):
        adapter = BinanceAdapter(
            symbols=["APEUSDT", "XRPUSDT"],
            on_snapshot=lambda s: None,
        )
        url = adapter._build_ws_url()
        assert "apeusdt@bookTicker" in url
        assert "xrpusdt@markPrice" in url
        # Streams separated by /
        assert "/" in url.split("streams=")[1]


# ---------------------------------------------------------------------------
# Stale detection and backoff
# ---------------------------------------------------------------------------

class TestStaleDetection:

    def test_not_stale_initially(self):
        adapter = BinanceAdapter(
            symbols=["APEUSDT"],
            on_snapshot=lambda s: None,
            stale_threshold_seconds=5.0,
        )
        assert adapter._is_stale() is False

    def test_stale_after_threshold(self):
        adapter = BinanceAdapter(
            symbols=["APEUSDT"],
            on_snapshot=lambda s: None,
            stale_threshold_seconds=5.0,
        )
        adapter._last_message_time = datetime.now(timezone.utc) - timedelta(seconds=10)
        assert adapter._is_stale() is True

    def test_not_stale_within_threshold(self):
        adapter = BinanceAdapter(
            symbols=["APEUSDT"],
            on_snapshot=lambda s: None,
            stale_threshold_seconds=5.0,
        )
        adapter._last_message_time = datetime.now(timezone.utc) - timedelta(seconds=2)
        assert adapter._is_stale() is False


class TestExponentialBackoff:

    def test_initial_delay(self):
        backoff = ExponentialBackoff(base_delay=1.0, max_delay=60.0, jitter=0)
        delay = backoff._next_delay()
        assert delay == 1.0

    def test_exponential_growth(self):
        backoff = ExponentialBackoff(base_delay=1.0, max_delay=60.0, jitter=0)
        delays = []
        for _ in range(5):
            delays.append(backoff._next_delay())
            backoff._attempt += 1
        # 1, 2, 4, 8, 16
        assert delays == [1.0, 2.0, 4.0, 8.0, 16.0]

    def test_max_cap(self):
        backoff = ExponentialBackoff(base_delay=1.0, max_delay=10.0, jitter=0)
        backoff._attempt = 100
        assert backoff._next_delay() == 10.0

    def test_reset(self):
        backoff = ExponentialBackoff(base_delay=1.0, max_delay=60.0, jitter=0)
        backoff._attempt = 5
        backoff.reset()
        assert backoff._next_delay() == 1.0

    def test_jitter_adds_randomness(self):
        backoff = ExponentialBackoff(base_delay=1.0, max_delay=60.0, jitter=1.0)
        delay = backoff._next_delay()
        # With jitter=1.0, delay should be between 1.0 and 2.0
        assert 1.0 <= delay <= 2.0
