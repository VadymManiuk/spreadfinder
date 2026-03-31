"""
Tests for Hyperliquid adapter message parsing and snapshot emission.

Covers: l2Book parsing, meta parsing, snapshot emission,
state merging, edge cases.
Uses raw JSON fixtures — no live WebSocket connection needed.
"""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from exchange_adapters.hyperliquid import HyperliquidAdapter
from models.snapshot import MarketSnapshot


# ---------------------------------------------------------------------------
# Fixtures — raw Hyperliquid payloads
# ---------------------------------------------------------------------------

L2_BOOK_RAW = {
    "coin": "APE",
    "levels": [
        [{"px": "1.2340", "sz": "500.0", "n": 5}, {"px": "1.2330", "sz": "300.0", "n": 3}],
        [{"px": "1.2350", "sz": "750.0", "n": 4}, {"px": "1.2360", "sz": "400.0", "n": 2}],
    ],
    "time": 1704067200000,  # 2024-01-01 00:00:00 UTC
}

L2_BOOK_RAW_2 = {
    "coin": "DOGE",
    "levels": [
        [{"px": "0.08500", "sz": "100000.0", "n": 10}],
        [{"px": "0.08510", "sz": "80000.0", "n": 8}],
    ],
    "time": 1704067200000,
}

L2_BOOK_EMPTY_BIDS = {
    "coin": "APE",
    "levels": [[], [{"px": "1.2350", "sz": "750.0", "n": 4}]],
}

L2_BOOK_MISSING_LEVELS = {
    "coin": "APE",
    "levels": [[{"px": "1.2340", "sz": "500.0", "n": 5}]],
}


# ---------------------------------------------------------------------------
# Static parsing tests
# ---------------------------------------------------------------------------

class TestL2BookParsing:

    def test_parse_basic(self):
        result = HyperliquidAdapter.parse_l2_book(L2_BOOK_RAW)
        assert result is not None
        assert result["coin"] == "APE"
        assert result["bid"] == Decimal("1.2340")
        assert result["ask"] == Decimal("1.2350")
        assert result["bid_size"] == Decimal("500.0")
        assert result["ask_size"] == Decimal("750.0")

    def test_parse_exchange_ts(self):
        result = HyperliquidAdapter.parse_l2_book(L2_BOOK_RAW)
        expected = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert result["exchange_ts"] == expected

    def test_parse_no_timestamp(self):
        data = {
            "coin": "APE",
            "levels": [
                [{"px": "1.00", "sz": "10", "n": 1}],
                [{"px": "1.01", "sz": "10", "n": 1}],
            ],
        }
        result = HyperliquidAdapter.parse_l2_book(data)
        assert "exchange_ts" not in result

    def test_parse_small_cap_precision(self):
        data = {
            "coin": "PEPE",
            "levels": [
                [{"px": "0.00001234", "sz": "50000000", "n": 1}],
                [{"px": "0.00001235", "sz": "30000000", "n": 1}],
            ],
        }
        result = HyperliquidAdapter.parse_l2_book(data)
        assert result["bid"] == Decimal("0.00001234")
        assert result["bid_size"] == Decimal("50000000")

    def test_parse_empty_bids_returns_none(self):
        result = HyperliquidAdapter.parse_l2_book(L2_BOOK_EMPTY_BIDS)
        assert result is None

    def test_parse_missing_levels_returns_none(self):
        result = HyperliquidAdapter.parse_l2_book(L2_BOOK_MISSING_LEVELS)
        assert result is None

    def test_parse_empty_data(self):
        result = HyperliquidAdapter.parse_l2_book({"coin": "APE", "levels": []})
        assert result is None


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

        return HyperliquidAdapter(
            symbols=["APE", "DOGE"],
            on_snapshot=collector,
            canonical_map={
                "APE": "APE-USDC-PERP",
                "DOGE": "DOGE-USDC-PERP",
            },
        )

    @pytest.mark.asyncio
    async def test_l2_book_emits_snapshot(self, adapter, collected_snapshots):
        await adapter._handle_l2_book(L2_BOOK_RAW)
        assert len(collected_snapshots) == 1
        snap = collected_snapshots[0]
        assert snap.canonical_symbol == "APE-USDC-PERP"
        assert snap.exchange == "hyperliquid"
        assert snap.bid == Decimal("1.2340")
        assert snap.ask == Decimal("1.2350")
        assert snap.bid_size == Decimal("500.0")
        assert snap.ask_size == Decimal("750.0")

    @pytest.mark.asyncio
    async def test_exchange_ts_set(self, adapter, collected_snapshots):
        await adapter._handle_l2_book(L2_BOOK_RAW)
        expected = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert collected_snapshots[0].exchange_ts == expected

    @pytest.mark.asyncio
    async def test_multiple_symbols_isolated(self, adapter, collected_snapshots):
        await adapter._handle_l2_book(L2_BOOK_RAW)
        await adapter._handle_l2_book(L2_BOOK_RAW_2)
        assert len(collected_snapshots) == 2
        assert collected_snapshots[0].canonical_symbol == "APE-USDC-PERP"
        assert collected_snapshots[1].canonical_symbol == "DOGE-USDC-PERP"

    @pytest.mark.asyncio
    async def test_empty_bids_no_emit(self, adapter, collected_snapshots):
        await adapter._handle_l2_book(L2_BOOK_EMPTY_BIDS)
        assert len(collected_snapshots) == 0

    @pytest.mark.asyncio
    async def test_missing_levels_no_emit(self, adapter, collected_snapshots):
        await adapter._handle_l2_book(L2_BOOK_MISSING_LEVELS)
        assert len(collected_snapshots) == 0

    @pytest.mark.asyncio
    async def test_snapshot_not_stale(self, adapter, collected_snapshots):
        await adapter._handle_l2_book(L2_BOOK_RAW)
        assert collected_snapshots[0].is_stale is False

    @pytest.mark.asyncio
    async def test_meta_enriches_state(self, adapter, collected_snapshots):
        """Simulate meta data enriching the state before a book update."""
        # Manually set meta state as if _fetch_meta ran
        adapter._state["APE"] = {
            "mark_price": Decimal("1.2345"),
            "index_price": Decimal("1.2343"),
            "funding_rate": Decimal("0.0001"),
            "volume_24h": Decimal("5000000"),
        }
        await adapter._handle_l2_book(L2_BOOK_RAW)
        snap = collected_snapshots[0]
        assert snap.mark_price == Decimal("1.2345")
        assert snap.index_price == Decimal("1.2343")
        assert snap.funding_rate == Decimal("0.0001")
        assert snap.volume_24h == Decimal("5000000")

    @pytest.mark.asyncio
    async def test_no_canonical_map_uses_native(self):
        snaps = []

        async def collector(snap):
            snaps.append(snap)

        adapter = HyperliquidAdapter(
            symbols=["APE"],
            on_snapshot=collector,
            canonical_map={},
        )
        await adapter._handle_l2_book(L2_BOOK_RAW)
        assert snaps[0].canonical_symbol == "APE"

    @pytest.mark.asyncio
    async def test_uses_top_of_book_only(self, adapter, collected_snapshots):
        """Should use only the best (first) level, not deeper levels."""
        await adapter._handle_l2_book(L2_BOOK_RAW)
        snap = collected_snapshots[0]
        # First bid level: 1.2340, not second: 1.2330
        assert snap.bid == Decimal("1.2340")
        # First ask level: 1.2350, not second: 1.2360
        assert snap.ask == Decimal("1.2350")
