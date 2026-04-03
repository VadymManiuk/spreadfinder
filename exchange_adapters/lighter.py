"""
Lighter (zkLighter) Perpetual Futures WebSocket adapter.

Inputs: List of symbols to subscribe to, snapshot callback.
Outputs: Normalized MarketSnapshot objects via callback.
Assumptions:
  - Markets identified by index number, fetched from REST API.
  - Subscribes to ticker/{MARKET_INDEX} for best bid/ask.
  - Single WS connection, all subscriptions via JSON messages.
  - Must send a frame every 2 minutes to keep alive.
  - Symbols are base asset only (e.g. "ETH", "BTC"), quoted in USDC.
"""

import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import websockets
import structlog

from exchange_adapters.base import BaseExchangeAdapter, SnapshotCallback
from models.snapshot import MarketSnapshot

logger = structlog.get_logger(__name__)

WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
REST_URL = "https://explorer.elliot.ai/api/markets"

PING_INTERVAL_SECONDS = 60  # Must send frame every 2 min; ping at 1 min


class LighterAdapter(BaseExchangeAdapter):
    """
    Lighter (zkLighter) Perpetual Futures WebSocket adapter.

    Subscribes to ticker/{market_index} for each market.
    Ticker provides best bid/ask with price and size.
    """

    def __init__(
        self,
        symbols: list[str],
        on_snapshot: SnapshotCallback,
        canonical_map: dict[str, str] | None = None,
        market_index_map: dict[str, int] | None = None,
        stale_threshold_seconds: float = 10.0,
    ):
        """
        Args:
            symbols: Native Lighter symbols (e.g. ["ETH", "BTC"]).
            on_snapshot: Async callback.
            canonical_map: {native: canonical} mapping.
            market_index_map: {native_symbol: market_index} for WS subscriptions.
        """
        super().__init__(
            exchange_name="lighter",
            on_snapshot=on_snapshot,
            stale_threshold_seconds=stale_threshold_seconds,
        )
        self._symbols = symbols
        self._canonical_map = canonical_map or {}
        self._market_index_map = market_index_map or {}
        # Reverse: index -> native symbol for WS message handling
        self._index_to_symbol: dict[int, str] = {v: k for k, v in self._market_index_map.items()}
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._state: dict[str, dict] = {}
        self._ping_task: asyncio.Task | None = None

    async def _connect(self) -> None:
        self._log.info("connecting", url=WS_URL, symbol_count=len(self._symbols))
        self._ws = await websockets.connect(
            WS_URL,
            ping_interval=None,
            ping_timeout=None,
            close_timeout=5,
        )
        self._log.info("connected")

    async def _disconnect(self) -> None:
        if self._ping_task:
            self._ping_task.cancel()
            self._ping_task = None
        if self._ws:
            await self._ws.close()
            self._ws = None

    async def _subscribe(self) -> None:
        if not self._ws:
            return

        for sym in self._symbols:
            idx = self._market_index_map.get(sym)
            if idx is None:
                self._log.warning("no_market_index", symbol=sym)
                continue
            msg = json.dumps({"type": "subscribe", "channel": f"ticker/{idx}"})
            await self._ws.send(msg)
            await asyncio.sleep(0.05)  # small delay to avoid flooding

        self._log.info("subscribed", symbol_count=len(self._symbols))
        self._ping_task = asyncio.create_task(self._ping_loop())

    async def _ping_loop(self) -> None:
        """Send ping every 60s to keep connection alive (Lighter requires frame every 2 min)."""
        while self._running and self._ws:
            await asyncio.sleep(PING_INTERVAL_SECONDS)
            try:
                if self._ws:
                    await self._ws.ping()
            except Exception:
                self._log.warning("ping_failed", exc_info=True)
                return

    async def _listen(self) -> None:
        if not self._ws:
            return

        async for raw in self._ws:
            self._update_heartbeat()
            try:
                msg = json.loads(raw)
                msg_type = msg.get("type", "")

                # Skip connection and subscription confirmations
                if msg_type in ("connected", "subscribed/ticker"):
                    continue

                if msg_type == "update/ticker":
                    await self._handle_ticker(msg)

            except (json.JSONDecodeError, InvalidOperation):
                self._log.warning("parse_error", raw=str(raw)[:200])
            except Exception:
                self._log.exception("message_handler_error")

    async def _handle_ticker(self, msg: dict) -> None:
        """
        Process Lighter ticker update.

        Format:
          {"channel": "ticker:0", "ticker": {"s": "ETH",
           "a": {"price": "2064.48", "size": "0.4950"},
           "b": {"price": "2064.30", "size": "1.0392"},
           "last_updated_at": 1774883844921166},
           "timestamp": 1774883844933, "type": "update/ticker"}
        """
        ticker = msg.get("ticker", {})
        symbol = ticker.get("s", "")

        if not symbol or symbol not in self._canonical_map:
            return

        if symbol not in self._state:
            self._state[symbol] = {}
        state = self._state[symbol]

        ask = ticker.get("a", {})
        bid = ticker.get("b", {})

        if ask.get("price"):
            state["ask"] = Decimal(ask["price"])
        if ask.get("size"):
            state["ask_size"] = Decimal(ask["size"])
        if bid.get("price"):
            state["bid"] = Decimal(bid["price"])
        if bid.get("size"):
            state["bid_size"] = Decimal(bid["size"])

        ts = msg.get("timestamp")
        if ts:
            # Lighter timestamp is in milliseconds
            state["exchange_ts"] = datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc)

        await self._emit_snapshot(symbol)

    async def _emit_snapshot(self, native_symbol: str) -> None:
        state = self._state.get(native_symbol, {})
        bid = state.get("bid")
        ask = state.get("ask")
        if bid is None or ask is None:
            return

        canonical = self._canonical_map.get(native_symbol, native_symbol)

        snapshot = MarketSnapshot(
            canonical_symbol=canonical,
            exchange="lighter",
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
            is_stale=False,
        )
        await self.on_snapshot(snapshot)
