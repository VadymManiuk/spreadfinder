"""
Bitget USDT-Futures WebSocket adapter.

Inputs: List of symbols to subscribe to, snapshot callback.
Outputs: Normalized MarketSnapshot objects via callback.
Assumptions:
  - Uses USDT-margined perpetual futures (v2 API).
  - Symbol format: "BTCUSDT" (same as Binance).
  - Subscribes to ticker channel for bid/ask + volume.
  - Bitget ping is literal string "ping", response is "pong".
"""

import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import websockets
import websockets.exceptions
import structlog

from exchange_adapters.base import BaseExchangeAdapter, SnapshotCallback
from models.snapshot import MarketSnapshot

logger = structlog.get_logger(__name__)

WS_URL = "wss://ws.bitget.com/v2/ws/public"

SUBSCRIBE_BATCH_SIZE = 20

PING_INTERVAL_SECONDS = 25


class BitgetAdapter(BaseExchangeAdapter):
    """
    Bitget USDT-Futures WebSocket adapter.

    Subscribes to "ticker" channel with instType "USDT-FUTURES" per symbol.
    Ticker provides bestBid, bestAsk, volume in one message.
    """

    def __init__(
        self,
        symbols: list[str],
        on_snapshot: SnapshotCallback,
        canonical_map: dict[str, str] | None = None,
        stale_threshold_seconds: float = 10.0,
    ):
        super().__init__(
            exchange_name="bitget",
            on_snapshot=on_snapshot,
            stale_threshold_seconds=stale_threshold_seconds,
        )
        self._symbols = symbols  # e.g. ["BTCUSDT", "ETHUSDT"]
        self._canonical_map = canonical_map or {}
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

        for i in range(0, len(self._symbols), SUBSCRIBE_BATCH_SIZE):
            batch = self._symbols[i : i + SUBSCRIBE_BATCH_SIZE]
            args = [
                {"instType": "USDT-FUTURES", "channel": "ticker", "instId": sym}
                for sym in batch
            ]
            msg = json.dumps({"op": "subscribe", "args": args})
            await self._ws.send(msg)
            await asyncio.sleep(0.1)

        self._log.info("subscribed", symbol_count=len(self._symbols))
        self._ping_task = asyncio.create_task(self._ping_loop())

    async def _ping_loop(self) -> None:
        """Bitget uses literal string "ping" for keepalive."""
        while self._running and self._ws:
            await asyncio.sleep(PING_INTERVAL_SECONDS)
            try:
                if self._ws:
                    await self._ws.send("ping")
            except Exception:
                self._log.warning("ping_failed", exc_info=True)
                return

    async def _listen(self) -> None:
        if not self._ws:
            return

        async for raw in self._ws:
            self._update_heartbeat()

            # Bitget pong is literal string "pong"
            if raw == "pong":
                continue

            try:
                msg = json.loads(raw)

                # Skip subscription confirmations
                if msg.get("event") in ("subscribe", "unsubscribe", "error"):
                    if msg.get("event") == "error":
                        self._log.warning("bitget_error", msg=msg.get("msg", ""))
                    continue

                action = msg.get("action", "")
                data_list = msg.get("data", [])

                if action in ("snapshot", "update") and data_list:
                    for data in data_list:
                        await self._handle_ticker(data)

            except (json.JSONDecodeError, InvalidOperation):
                self._log.warning("parse_error", raw=str(raw)[:200])
            except Exception:
                self._log.exception("message_handler_error")

    async def _handle_ticker(self, data: dict) -> None:
        """
        Process Bitget ticker update.

        Fields: instId, bestBid, bestBidSz, bestAsk, bestAskSz,
                baseVolume, quoteVolume, ts
        """
        symbol = data.get("instId", "")
        if not symbol:
            return

        if symbol not in self._state:
            self._state[symbol] = {}
        state = self._state[symbol]

        if data.get("bestBid"):
            state["bid"] = Decimal(data["bestBid"])
        if data.get("bestAsk"):
            state["ask"] = Decimal(data["bestAsk"])
        if data.get("bestBidSz"):
            state["bid_size"] = Decimal(data["bestBidSz"])
        if data.get("bestAskSz"):
            state["ask_size"] = Decimal(data["bestAskSz"])
        if data.get("quoteVolume"):
            state["volume_24h"] = Decimal(data["quoteVolume"])

        ts = data.get("ts")
        if ts:
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
            exchange="bitget",
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
