"""
MEXC Perpetual Futures WebSocket adapter.

Inputs: List of symbols to subscribe to (e.g. ["BTC_USDT"]), snapshot callback.
Outputs: Normalized MarketSnapshot objects via callback.
Assumptions:
  - Uses USDT-margined perpetual futures.
  - Symbol format: "BTC_USDT" (underscore-separated, like Gate).
  - Subscribes to ticker channel via {"method": "sub.ticker", "param": {"symbol": ...}}.
  - Ticker provides bid1, ask1, fundingRate, fairPrice, indexPrice, amount24.
  - No bid/ask size in ticker — uses Decimal(0) as placeholder.
  - Ping: sends {"method": "ping"} every 25s (no special pong — server sends ticker data).
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

WS_URL = "wss://contract.mexc.com/edge"

# MEXC allows many subscriptions per connection
PING_INTERVAL_SECONDS = 25


class MexcAdapter(BaseExchangeAdapter):
    """
    MEXC Perpetual Futures WebSocket adapter.

    Subscribes to "sub.ticker" per symbol. Each ticker push contains:
      bid1, ask1, fundingRate, fairPrice, indexPrice, amount24 (quote volume).
    """

    def __init__(
        self,
        symbols: list[str],
        on_snapshot: SnapshotCallback,
        canonical_map: dict[str, str] | None = None,
        stale_threshold_seconds: float = 10.0,
    ):
        super().__init__(
            exchange_name="mexc",
            on_snapshot=on_snapshot,
            stale_threshold_seconds=stale_threshold_seconds,
        )
        self._symbols = symbols  # e.g. ["BTC_USDT", "ETH_USDT"]
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

        # MEXC uses individual subscribe messages per symbol
        for sym in self._symbols:
            msg = json.dumps({"method": "sub.ticker", "param": {"symbol": sym}})
            await self._ws.send(msg)
            # Small delay to avoid flooding
            if len(self._symbols) > 100:
                await asyncio.sleep(0.02)

        self._log.info("subscribed", symbol_count=len(self._symbols))
        self._ping_task = asyncio.create_task(self._ping_loop())

    async def _ping_loop(self) -> None:
        """Send periodic ping to keep connection alive."""
        while self._running and self._ws:
            await asyncio.sleep(PING_INTERVAL_SECONDS)
            try:
                if self._ws:
                    await self._ws.send(json.dumps({"method": "ping"}))
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

                # Skip subscription confirmations
                channel = msg.get("channel", "")
                if channel == "rs.sub.ticker":
                    continue

                # Ticker push: {"symbol": "BTC_USDT", "data": {...}, "channel": "push.ticker"}
                if "data" in msg and isinstance(msg["data"], dict):
                    await self._handle_ticker(msg["data"])

            except (json.JSONDecodeError, InvalidOperation):
                self._log.warning("parse_error", raw=str(raw)[:200])
            except Exception:
                self._log.exception("message_handler_error")

    async def _handle_ticker(self, data: dict) -> None:
        """
        Process MEXC ticker update.

        Ticker fields:
          symbol, lastPrice, bid1, ask1, fairPrice, indexPrice,
          fundingRate, volume24, amount24, holdVol, timestamp
        """
        symbol = data.get("symbol", "")
        if not symbol or symbol not in self._canonical_map:
            return

        if symbol not in self._state:
            self._state[symbol] = {}
        state = self._state[symbol]

        # bid/ask (no sizes available from MEXC ticker)
        bid1 = data.get("bid1")
        if bid1 is not None:
            state["bid"] = Decimal(str(bid1))
        ask1 = data.get("ask1")
        if ask1 is not None:
            state["ask"] = Decimal(str(ask1))

        # Mark price (fairPrice) and index price
        fair = data.get("fairPrice")
        if fair is not None:
            state["mark_price"] = Decimal(str(fair))
        index = data.get("indexPrice")
        if index is not None:
            state["index_price"] = Decimal(str(index))

        # Funding rate
        fr = data.get("fundingRate")
        if fr is not None:
            state["funding_rate"] = Decimal(str(fr))

        # Volume: amount24 is quote volume (USDT)
        amount = data.get("amount24")
        if amount is not None:
            state["volume_24h"] = Decimal(str(amount))

        # Timestamp
        ts = data.get("timestamp")
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
            exchange="mexc",
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
