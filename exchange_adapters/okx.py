"""
OKX Perpetual Swap WebSocket adapter.

Inputs: List of instrument IDs to subscribe to, snapshot callback.
Outputs: Normalized MarketSnapshot objects via callback.
Assumptions:
  - Uses USDT-margined linear perpetual swaps.
  - Instrument format: "BTC-USDT-SWAP".
  - Subscribes to tickers channel for bid/ask + mark/funding/volume.
  - OKX ping is literal string "ping", response is "pong".
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

WS_URL = "wss://ws.okx.com:8443/ws/v5/public"

# OKX allows subscribing to many instruments per connection.
SUBSCRIBE_BATCH_SIZE = 20

PING_INTERVAL_SECONDS = 25


class OkxAdapter(BaseExchangeAdapter):
    """
    OKX Perpetual Swap WebSocket adapter.

    Subscribes to "tickers" channel per instrument.
    Ticker provides bid/ask, volume, and last price in one message.
    Mark price and funding come from a separate REST poll or the
    "mark-price" channel — here we use tickers for simplicity since
    the spread engine only needs bid/ask.
    """

    def __init__(
        self,
        symbols: list[str],
        on_snapshot: SnapshotCallback,
        canonical_map: dict[str, str] | None = None,
        stale_threshold_seconds: float = 10.0,
    ):
        super().__init__(
            exchange_name="okx",
            on_snapshot=on_snapshot,
            stale_threshold_seconds=stale_threshold_seconds,
        )
        self._symbols = symbols  # e.g. ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]
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

        # Subscribe in batches
        for i in range(0, len(self._symbols), SUBSCRIBE_BATCH_SIZE):
            batch = self._symbols[i : i + SUBSCRIBE_BATCH_SIZE]
            args = [{"channel": "tickers", "instId": sym} for sym in batch]
            msg = json.dumps({"op": "subscribe", "args": args})
            await self._ws.send(msg)
            await asyncio.sleep(0.1)

        self._log.info("subscribed", symbol_count=len(self._symbols))
        self._ping_task = asyncio.create_task(self._ping_loop())

    async def _ping_loop(self) -> None:
        """OKX uses literal string "ping" for keepalive."""
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

            # OKX pong is literal string "pong"
            if raw == "pong":
                continue

            try:
                msg = json.loads(raw)

                # Skip subscription confirmations
                if msg.get("event") in ("subscribe", "unsubscribe", "error"):
                    if msg.get("event") == "error":
                        self._log.warning("okx_error", msg=msg.get("msg", ""))
                    continue

                arg = msg.get("arg", {})
                channel = arg.get("channel", "")
                data_list = msg.get("data", [])

                if channel == "tickers" and data_list:
                    for data in data_list:
                        await self._handle_ticker(data)

            except (json.JSONDecodeError, InvalidOperation):
                self._log.warning("parse_error", raw=str(raw)[:200])
            except Exception:
                self._log.exception("message_handler_error")

    async def _handle_ticker(self, data: dict) -> None:
        """
        Process OKX ticker update.

        Fields: instId, bidPx, bidSz, askPx, askSz, last, vol24h, volCcy24h, ts
        """
        inst_id = data.get("instId", "")
        if not inst_id:
            return

        if inst_id not in self._state:
            self._state[inst_id] = {}
        state = self._state[inst_id]

        if data.get("bidPx"):
            state["bid"] = Decimal(data["bidPx"])
        if data.get("askPx"):
            state["ask"] = Decimal(data["askPx"])
        if data.get("bidSz"):
            state["bid_size"] = Decimal(data["bidSz"])
        if data.get("askSz"):
            state["ask_size"] = Decimal(data["askSz"])
        if data.get("volCcy24h"):
            state["volume_24h"] = Decimal(data["volCcy24h"])

        ts = data.get("ts")
        if ts:
            state["exchange_ts"] = datetime.fromtimestamp(int(ts) / 1000, tz=timezone.utc)

        await self._emit_snapshot(inst_id)

    async def _emit_snapshot(self, native_symbol: str) -> None:
        state = self._state.get(native_symbol, {})
        bid = state.get("bid")
        ask = state.get("ask")
        if bid is None or ask is None:
            return

        canonical = self._canonical_map.get(native_symbol, native_symbol)

        snapshot = MarketSnapshot(
            canonical_symbol=canonical,
            exchange="okx",
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
