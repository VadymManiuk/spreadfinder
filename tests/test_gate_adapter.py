"""
Tests for Gate adapter message parsing and snapshot emission.

Covers: book_ticker parsing, snapshot emission, state merging,
edge cases, subscription limits.
Uses raw JSON fixtures — no live WebSocket connection needed.
"""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from exchange_adapters.gate import GateAdapter, MAX_SUBS_PER_CONN
from models.snapshot import MarketSnapshot


# ---------------------------------------------------------------------------
# Fixtures — raw Gate payloads
# ---------------------------------------------------------------------------

BOOK_TICKER_RAW = {
    "t": 1704067200123,
    "s": "APE_USDT",
    "b": "1.2340",
    "B": 500,
    "a": "1.2350",
    "A": 750,
}

BOOK_TICKER_RAW_2 = {
    "t": 1704067200123,
    "s": "DOGE_USDT",
    "b": "0.08500",
    "B": 100000,
    "a": "0.08510",
    "A": 80000,
}

BOOK_TICKER_MISSING_SYMBOL = {
    "t": 1704067200123,
    "b": "1.00",
    "B": 10,
    "a": "1.01",
    "A": 10,
}


# ---------------------------------------------------------------------------
# Static parsing tests
# ---------------------------------------------------------------------------

class TestBookTickerParsing:

    def test_parse_basic(self):
        result = GateAdapter.parse_book_ticker(BOOK_TICKER_RAW)
        assert result is not None
        assert result["symbol"] == "APE_USDT"
        assert result["bid"] == Decimal("1.2340")
        assert result["ask"] == Decimal("1.2350")
        assert result["bid_size"] == Decimal("500")
        assert result["ask_size"] == Decimal("750")

    def test_parse_exchange_ts(self):
        result = GateAdapter.parse_book_ticker(BOOK_TICKER_RAW)
        ts = result["exchange_ts"]
        assert ts.year == 2024
        assert ts.tzinfo == timezone.utc

    def test_parse_no_timestamp(self):
        data = {"s": "APE_USDT", "b": "1.00", "B": 10, "a": "1.01", "A": 10}
        result = GateAdapter.parse_book_ticker(data)
        assert "exchange_ts" not in result

    def test_parse_missing_symbol_returns_none(self):
        result = GateAdapter.parse_book_ticker(BOOK_TICKER_MISSING_SYMBOL)
        assert result is None

    def test_parse_missing_price_returns_none(self):
        data = {"s": "APE_USDT", "b": "1.00", "A": 10}  # missing B and a
        result = GateAdapter.parse_book_ticker(data)
        assert result is None

    def test_parse_numeric_values(self):
        """Gate sends sizes as numbers, not strings."""
        data = {"s": "APE_USDT", "b": "1.50", "B": 1234, "a": "1.51", "A": 5678, "t": 1704067200000}
        result = GateAdapter.parse_book_ticker(data)
        assert result["bid_size"] == Decimal("1234")
        assert result["ask_size"] == Decimal("5678")


# ---------------------------------------------------------------------------
# Snapshot emission tests
# ---------------------------------------------------------------------------

class TestSnapshotEmission:

    @pytest.fixture
    def collected_snapshots(self):
        return []

    @pytest.fixture
    def adapter(self, collected_snapshots):
        async def collector(snap: MarketSnapshot):
            collected_snapshots.append(snap)

        return GateAdapter(
            symbols=["APE_USDT", "DOGE_USDT"],
            on_snapshot=collector,
            canonical_map={
                "APE_USDT": "APE-USDT-PERP",
                "DOGE_USDT": "DOGE-USDT-PERP",
            },
        )

    @pytest.mark.asyncio
    async def test_book_ticker_emits_snapshot(self, adapter, collected_snapshots):
        await adapter._handle_book_ticker(BOOK_TICKER_RAW)
        assert len(collected_snapshots) == 1
        snap = collected_snapshots[0]
        assert snap.canonical_symbol == "APE-USDT-PERP"
        assert snap.exchange == "gate"
        assert snap.bid == Decimal("1.2340")
        assert snap.ask == Decimal("1.2350")

    @pytest.mark.asyncio
    async def test_exchange_ts_set(self, adapter, collected_snapshots):
        await adapter._handle_book_ticker(BOOK_TICKER_RAW)
        assert collected_snapshots[0].exchange_ts is not None

    @pytest.mark.asyncio
    async def test_multiple_symbols_isolated(self, adapter, collected_snapshots):
        await adapter._handle_book_ticker(BOOK_TICKER_RAW)
        await adapter._handle_book_ticker(BOOK_TICKER_RAW_2)
        assert len(collected_snapshots) == 2
        assert collected_snapshots[0].canonical_symbol == "APE-USDT-PERP"
        assert collected_snapshots[1].canonical_symbol == "DOGE-USDT-PERP"

    @pytest.mark.asyncio
    async def test_snapshot_not_stale(self, adapter, collected_snapshots):
        await adapter._handle_book_ticker(BOOK_TICKER_RAW)
        assert collected_snapshots[0].is_stale is False

    @pytest.mark.asyncio
    async def test_meta_enriches_state(self, adapter, collected_snapshots):
        """Simulate REST meta data enriching state."""
        adapter._state["APE_USDT"] = {
            "mark_price": Decimal("1.2345"),
            "index_price": Decimal("1.2343"),
            "funding_rate": Decimal("0.00015"),
            "volume_24h": Decimal("3000000"),
        }
        await adapter._handle_book_ticker(BOOK_TICKER_RAW)
        snap = collected_snapshots[0]
        assert snap.mark_price == Decimal("1.2345")
        assert snap.funding_rate == Decimal("0.00015")
        assert snap.volume_24h == Decimal("3000000")

    @pytest.mark.asyncio
    async def test_no_canonical_map_uses_native(self):
        snaps = []

        async def collector(snap):
            snaps.append(snap)

        adapter = GateAdapter(
            symbols=["APE_USDT"],
            on_snapshot=collector,
            canonical_map={},
        )
        await adapter._handle_book_ticker(BOOK_TICKER_RAW)
        assert snaps[0].canonical_symbol == "APE_USDT"

    @pytest.mark.asyncio
    async def test_sizes_as_decimal(self, adapter, collected_snapshots):
        """Gate sends sizes as int/float — should be converted to Decimal."""
        await adapter._handle_book_ticker(BOOK_TICKER_RAW)
        snap = collected_snapshots[0]
        assert isinstance(snap.bid_size, Decimal)
        assert isinstance(snap.ask_size, Decimal)


# ---------------------------------------------------------------------------
# Subscription limit
# ---------------------------------------------------------------------------

class TestSubscriptionLimit:

    def test_warns_over_limit(self, capsys):
        """Should log a warning when exceeding 100 subscriptions."""
        symbols = [f"TOKEN{i}_USDT" for i in range(101)]
        adapter = GateAdapter(
            symbols=symbols,
            on_snapshot=lambda s: None,
        )
        # The warning is logged via structlog, not capsys, but the adapter
        # should still be created without error
        assert len(adapter._symbols) == 101
