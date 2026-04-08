"""
Hyperliquid perpetual futures WebSocket adapter.

Inputs: List of symbols to subscribe to, snapshot callback.
Outputs: Normalized MarketSnapshot objects via callback.
Assumptions:
  - Hyperliquid symbols are just base asset names (e.g. "BTC", "ETH").
  - All contracts are quoted in USDC.
  - Uses l2Book subscription for best bid/ask (top-of-book from L2 data).
  - Uses REST POST to /info for metadata (funding rates, mark price, etc.).
  - WebSocket URL: wss://api.hyperliquid.xyz/ws

Rate limits (Hyperliquid WebSocket):
  - TODO — check official docs for exact rate limits
  - Subscriptions are sent as JSON messages after connecting
  - Each subscription is a separate message
"""

import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import aiohttp
import websockets
import websockets.exceptions
import structlog

from exchange_adapters.base import BaseExchangeAdapter, SnapshotCallback
from models.snapshot import MarketSnapshot

logger = structlog.get_logger(__name__)

WS_URL = "wss://api.hyperliquid.xyz/ws"
REST_URL = "https://api.hyperliquid.xyz/info"

# How often to poll REST for funding/mark prices (seconds)
# ESTIMATE — balance between freshness and rate limits
META_POLL_INTERVAL_SECONDS = 30


class HyperliquidAdapter(BaseExchangeAdapter):
    """
    Hyperliquid perpetual futures WebSocket adapter.

    Subscribes to l2Book channel for each symbol to get top-of-book bid/ask.
    Periodically polls REST /info endpoint for mark price, index price,
    and funding rate (not available via WebSocket).
    """

    def __init__(
        self,
        symbols: list[str],
        on_snapshot: SnapshotCallback,
        canonical_map: dict[str, str] | None = None,
        stale_threshold_seconds: float = 10.0,
    ):
        """
        Args:
            symbols: Native Hyperliquid symbols (e.g. ["BTC", "ETH", "APE"]).
            on_snapshot: Async callback receiving MarketSnapshot objects.
            canonical_map: Optional {native: canonical} mapping.
            stale_threshold_seconds: Seconds without data before marking feed stale.
        """
        super().__init__(
            exchange_name="hyperliquid",
            on_snapshot=on_snapshot,
            stale_threshold_seconds=stale_threshold_seconds,
        )
        self._symbols = symbols
        self._canonical_map = canonical_map or {}
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._meta_task: asyncio.Task | None = None
        self._http_session: aiohttp.ClientSession | None = None

        # In-memory state per symbol
        # {native_symbol: {field: value}}
        self._state: dict[str, dict] = {}

    async def _connect(self) -> None:
        """Establish WebSocket connection to Hyperliquid."""
        self._log.info("connecting", url=WS_URL, symbol_count=len(self._symbols))
        self._ws = await websockets.connect(
            WS_URL,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        )
        self._http_session = aiohttp.ClientSession()
        self._log.info("connected")

    async def _disconnect(self) -> None:
        """Close WebSocket and HTTP session."""
        if self._meta_task:
            self._meta_task.cancel()
            self._meta_task = None
        if self._ws:
            await self._ws.close()
            self._ws = None
        if self._http_session:
            await self._http_session.close()
            self._http_session = None

    async def _subscribe(self) -> None:
        """
        Subscribe to l2Book for each symbol.

        Subscription message format:
          {"method": "subscribe", "subscription": {"type": "l2Book", "coin": "BTC"}}
        """
        if not self._ws:
            return

        for symbol in self._symbols:
            msg = {
                "method": "subscribe",
                "subscription": {"type": "l2Book", "coin": symbol},
            }
            await self._ws.send(json.dumps(msg))
            self._log.debug("subscribed", symbol=symbol)

        # Start periodic meta polling for funding/mark prices
        self._meta_task = asyncio.create_task(self._meta_poll_loop())

    async def _listen(self) -> None:
        """Receive and process WebSocket messages."""
        if not self._ws:
            return

        async for raw_message in self._ws:
            self._update_heartbeat()
            try:
                message = json.loads(raw_message)
                channel = message.get("channel")
                data = message.get("data")

                if channel == "l2Book":
                    await self._handle_l2_book(data)

            except (json.JSONDecodeError, InvalidOperation):
                self._log.warning("parse_error", raw=str(raw_message)[:200])
            except Exception:
                self._log.exception("message_handler_error")

    async def _handle_l2_book(self, data: dict) -> None:
        """
        Process l2Book message and emit snapshot.

        l2Book payload:
          {"coin": "BTC", "levels": [
            [{"px": "50000.0", "sz": "1.5", "n": 3}, ...],  // bids
            [{"px": "50010.0", "sz": "2.0", "n": 2}, ...]   // asks
          ], "time": 1704067200000}
        """
        coin = data.get("coin", "")
        levels = data.get("levels", [])

        if len(levels) < 2:
            return

        bids = levels[0]
        asks = levels[1]

        if not bids or not asks:
            return

        if coin not in self._state:
            self._state[coin] = {}

        state = self._state[coin]
        state["bid"] = Decimal(bids[0]["px"])
        state["ask"] = Decimal(asks[0]["px"])
        state["bid_size"] = Decimal(bids[0]["sz"])
        state["ask_size"] = Decimal(asks[0]["sz"])

        ts_ms = data.get("time")
        if ts_ms:
            state["exchange_ts"] = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

        await self._emit_snapshot(coin)

    async def _meta_poll_loop(self) -> None:
        """Periodically fetch funding rates and mark prices via REST."""
        while self._running:
            try:
                await self._fetch_meta()
            except Exception:
                self._log.exception("meta_poll_error")
            await asyncio.sleep(META_POLL_INTERVAL_SECONDS)

    async def _fetch_meta(self) -> None:
        """
        Fetch metadata from Hyperliquid REST API.

        POST /info with {"type": "metaAndAssetCtxs"}
        Returns: [meta, [assetCtx, ...]]
          assetCtx: {"funding": "0.00010000", "markPx": "50005.0",
                     "oraclePx": "50003.0", "dayNtlVlm": "500000000", ...}
        """
        if not self._http_session:
            return

        async with self._http_session.post(
            REST_URL,
            json={"type": "metaAndAssetCtxs"},
            headers={"Content-Type": "application/json"},
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()

        if not isinstance(data, list) or len(data) < 2:
            return

        meta = data[0]
        asset_ctxs = data[1]
        universe = meta.get("universe", [])

        # Match asset contexts to symbols by index
        for i, ctx in enumerate(asset_ctxs):
            if i >= len(universe):
                break
            coin = universe[i].get("name", "")
            if coin not in self._state:
                continue

            state = self._state[coin]
            state["mark_price"] = Decimal(ctx.get("markPx", "0"))
            state["index_price"] = Decimal(ctx.get("oraclePx", "0"))
            state["funding_rate"] = Decimal(ctx.get("funding", "0"))

            day_vlm = ctx.get("dayNtlVlm")
            if day_vlm is not None:
                state["volume_24h"] = Decimal(str(day_vlm))

    async def _emit_snapshot(self, native_symbol: str) -> None:
        """Build and emit a MarketSnapshot from the current merged state."""
        state = self._state.get(native_symbol, {})
        bid = state.get("bid")
        ask = state.get("ask")

        if bid is None or ask is None:
            return

        canonical = self._canonical_map.get(native_symbol, native_symbol)

        snapshot = MarketSnapshot(
            canonical_symbol=canonical,
            exchange="hyperliquid",
            bid=bid,
            ask=ask,
            bid_size=state.get("bid_size", Decimal(0)),
            ask_size=state.get("ask_size", Decimal(0)),
            exchange_ts=state.get("exchange_ts"),
            local_ts=datetime.now(timezone.utc),
            mark_price=state.get("mark_price"),
            index_price=state.get("index_price"),
            funding_rate=state.get("funding_rate"),
            volume_24h=state.get("volume_24h"),
            next_funding_time=state.get("next_funding_time"),
            is_stale=False,
        )

        await self.on_snapshot(snapshot)

    @staticmethod
    def parse_l2_book(data: dict) -> dict | None:
        """
        Parse a raw l2Book payload into typed values.
        Useful for testing without a live connection.

        Returns dict with: coin, bid, ask, bid_size, ask_size, exchange_ts
        or None if data is insufficient.
        """
        coin = data.get("coin", "")
        levels = data.get("levels", [])

        if len(levels) < 2 or not levels[0] or not levels[1]:
            return None

        result = {
            "coin": coin,
            "bid": Decimal(levels[0][0]["px"]),
            "ask": Decimal(levels[1][0]["px"]),
            "bid_size": Decimal(levels[0][0]["sz"]),
            "ask_size": Decimal(levels[1][0]["sz"]),
        }

        ts_ms = data.get("time")
        if ts_ms:
            result["exchange_ts"] = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

        return result
