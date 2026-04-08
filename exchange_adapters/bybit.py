"""
Bybit Linear Perpetual WebSocket adapter.

Inputs: List of symbols to subscribe to, snapshot callback.
Outputs: Normalized MarketSnapshot objects via callback.
Assumptions:
  - Uses USDT-margined linear perpetuals (v5 API).
  - Subscribes to tickers.{SYMBOL} for best bid/ask + mark/index/funding.
  - Single WS connection handles many symbols via explicit subscribe messages.
  - Ping every 20 seconds to keep alive.
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

WS_URL = "wss://stream.bybit.com/v5/public/linear"

# Bybit allows many subscriptions per connection.
# Batch subscribe in groups to avoid message size limits.
SUBSCRIBE_BATCH_SIZE = 10

PING_INTERVAL_SECONDS = 20


class BybitAdapter(BaseExchangeAdapter):
    """
    Bybit Linear Perpetual WebSocket adapter.

    Subscribes to tickers.{SYMBOL} for each symbol.
    Ticker stream provides bid/ask, mark, index, funding, volume in one message.
    """

    def __init__(
        self,
        symbols: list[str],
        on_snapshot: SnapshotCallback,
        canonical_map: dict[str, str] | None = None,
        stale_threshold_seconds: float = 10.0,
    ):
        super().__init__(
            exchange_name="bybit",
            on_snapshot=on_snapshot,
            stale_threshold_seconds=stale_threshold_seconds,
        )
        self._symbols = symbols
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
            args = [f"tickers.{sym}" for sym in batch]
            msg = json.dumps({"op": "subscribe", "args": args})
            await self._ws.send(msg)
            await asyncio.sleep(0.1)  # small delay between batches

        self._log.info("subscribed", symbol_count=len(self._symbols))
        self._ping_task = asyncio.create_task(self._ping_loop())

    async def _ping_loop(self) -> None:
        while self._running and self._ws:
            await asyncio.sleep(PING_INTERVAL_SECONDS)
            try:
                if self._ws:
                    await self._ws.send(json.dumps({"op": "ping"}))
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

                # Skip subscription confirmations and pong responses
                if msg.get("op") in ("subscribe", "pong"):
                    continue
                if "success" in msg:
                    continue

                topic = msg.get("topic", "")
                data = msg.get("data")

                if topic.startswith("tickers.") and data:
                    await self._handle_ticker(data)

            except (json.JSONDecodeError, InvalidOperation):
                self._log.warning("parse_error", raw=str(raw)[:200])
            except Exception:
                self._log.exception("message_handler_error")

    async def _handle_ticker(self, data: dict) -> None:
        """
        Process ticker update. Bybit ticker contains bid/ask + mark/index/funding.

        Fields: symbol, bid1Price, bid1Size, ask1Price, ask1Size,
                markPrice, indexPrice, fundingRate, volume24h, turnover24h
        """
        symbol = data.get("symbol", "")
        if not symbol:
            return

        if symbol not in self._state:
            self._state[symbol] = {}
        state = self._state[symbol]

        # bid/ask (may be absent in delta updates)
        if "bid1Price" in data and data["bid1Price"]:
            state["bid"] = Decimal(data["bid1Price"])
        if "ask1Price" in data and data["ask1Price"]:
            state["ask"] = Decimal(data["ask1Price"])
        if "bid1Size" in data and data["bid1Size"]:
            state["bid_size"] = Decimal(data["bid1Size"])
        if "ask1Size" in data and data["ask1Size"]:
            state["ask_size"] = Decimal(data["ask1Size"])

        # mark/index/funding
        if "markPrice" in data and data["markPrice"]:
            state["mark_price"] = Decimal(data["markPrice"])
        if "indexPrice" in data and data["indexPrice"]:
            state["index_price"] = Decimal(data["indexPrice"])
        if "fundingRate" in data and data["fundingRate"]:
            state["funding_rate"] = Decimal(data["fundingRate"])

        # volume
        if "turnover24h" in data and data["turnover24h"]:
            state["volume_24h"] = Decimal(data["turnover24h"])

        # next funding settlement time
        nft = data.get("nextFundingTime")
        if nft and nft != "0":
            state["next_funding_time"] = datetime.fromtimestamp(int(nft) / 1000, tz=timezone.utc)

        # timestamp
        ts_ms = data.get("ts")
        if ts_ms:
            state["exchange_ts"] = datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)

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
            exchange="bybit",
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
