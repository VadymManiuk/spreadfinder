"""
Gate.io perpetual futures WebSocket adapter.

Inputs: List of symbols to subscribe to, snapshot callback.
Outputs: Normalized MarketSnapshot objects via callback.
Assumptions:
  - Gate futures symbols use underscore format: "BTC_USDT".
  - Uses futures.book_ticker channel for best bid/ask.
  - Uses REST for mark price, funding rate, and 24h volume.
  - WebSocket URL: wss://fx-ws.gateio.ws/v4/ws/usdt

Rate limits (Gate WebSocket):
  - Max 100 subscriptions per connection
  - Ping/pong handled by the websockets library
  - Subscriptions are batched in a single message per channel
"""

import asyncio
import json
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import aiohttp
import websockets
import websockets.exceptions
import structlog

from exchange_adapters.base import BaseExchangeAdapter, SnapshotCallback
from models.snapshot import MarketSnapshot

logger = structlog.get_logger(__name__)

WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
REST_BASE = "https://api.gateio.ws/api/v4/futures/usdt"

# Max subscriptions per connection (Gate limit: 100)
MAX_SUBS_PER_CONN = 100

# How often to poll REST for funding/mark prices (seconds)
META_POLL_INTERVAL_SECONDS = 30


class GateAdapter(BaseExchangeAdapter):
    """
    Gate.io USDT-margined perpetual futures WebSocket adapter.

    Subscribes to futures.book_ticker for best bid/ask.
    Periodically polls REST for mark price, funding rate, and volume.
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
            symbols: Native Gate symbols (e.g. ["BTC_USDT", "APE_USDT"]).
            on_snapshot: Async callback receiving MarketSnapshot objects.
            canonical_map: Optional {native: canonical} mapping.
            stale_threshold_seconds: Seconds without data before marking feed stale.
        """
        super().__init__(
            exchange_name="gate",
            on_snapshot=on_snapshot,
            stale_threshold_seconds=stale_threshold_seconds,
        )
        self._symbols = symbols
        self._canonical_map = canonical_map or {}
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._meta_task: asyncio.Task | None = None
        self._http_session: aiohttp.ClientSession | None = None

        # In-memory state per symbol
        self._state: dict[str, dict] = {}

        if len(symbols) > MAX_SUBS_PER_CONN:
            self._log.warning(
                "too_many_subscriptions",
                count=len(symbols),
                max=MAX_SUBS_PER_CONN,
                hint="Split symbols across multiple connections",
            )

    async def _connect(self) -> None:
        """Establish WebSocket connection to Gate futures."""
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
        Subscribe to futures.book_ticker for all symbols.

        Gate subscription format:
          {"time": <unix_ts>, "channel": "futures.book_ticker",
           "event": "subscribe", "payload": ["BTC_USDT", "APE_USDT"]}
        """
        if not self._ws:
            return

        msg = {
            "time": int(time.time()),
            "channel": "futures.book_ticker",
            "event": "subscribe",
            "payload": self._symbols,
        }
        await self._ws.send(json.dumps(msg))
        self._log.debug("subscribed", symbols=self._symbols)

        # Start periodic meta polling
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
                event = message.get("event")

                # Skip subscription confirmations and errors
                if event in ("subscribe", "unsubscribe"):
                    continue

                if channel == "futures.book_ticker" and event == "update":
                    result = message.get("result")
                    if result:
                        await self._handle_book_ticker(result)

            except (json.JSONDecodeError, InvalidOperation):
                self._log.warning("parse_error", raw=str(raw_message)[:200])
            except Exception:
                self._log.exception("message_handler_error")

    async def _handle_book_ticker(self, data: dict) -> None:
        """
        Process futures.book_ticker update and emit snapshot.

        book_ticker payload:
          {"t": 1704067200123, "s": "APE_USDT",
           "b": "1.2340", "B": 500, "a": "1.2350", "A": 750}

        Fields:
          t = timestamp (ms), s = contract name,
          b = best bid price, B = best bid size,
          a = best ask price, A = best ask size
        """
        symbol = data.get("s", "")

        if symbol not in self._state:
            self._state[symbol] = {}

        state = self._state[symbol]
        state["bid"] = Decimal(str(data["b"]))
        state["ask"] = Decimal(str(data["a"]))
        state["bid_size"] = Decimal(str(data["B"]))
        state["ask_size"] = Decimal(str(data["A"]))

        ts_ms = data.get("t")
        if ts_ms:
            state["exchange_ts"] = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

        await self._emit_snapshot(symbol)

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
        Fetch contract metadata from Gate REST API.

        GET /api/v4/futures/usdt/contracts/{contract}
        Returns: {"mark_price": "1.2345", "funding_rate": "0.0001",
                  "index_price": "1.2343", "trade_size": 500000, ...}

        Also fetches 24h tickers for volume data.
        """
        if not self._http_session:
            return

        # Fetch tickers for all contracts at once
        try:
            async with self._http_session.get(
                f"{REST_BASE}/tickers",
            ) as resp:
                resp.raise_for_status()
                tickers = await resp.json()

            for ticker in tickers:
                contract = ticker.get("contract", "")
                if contract not in self._state:
                    continue

                state = self._state[contract]

                mark = ticker.get("mark_price")
                if mark:
                    state["mark_price"] = Decimal(str(mark))

                index = ticker.get("index_price")
                if index:
                    state["index_price"] = Decimal(str(index))

                funding = ticker.get("funding_rate")
                if funding:
                    state["funding_rate"] = Decimal(str(funding))

                volume = ticker.get("volume_24h_quote")
                if volume:
                    state["volume_24h"] = Decimal(str(volume))

        except Exception:
            self._log.exception("ticker_fetch_error")

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
            exchange="gate",
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

    @staticmethod
    def parse_book_ticker(data: dict) -> dict | None:
        """
        Parse a raw Gate book_ticker payload into typed values.
        Useful for testing without a live connection.

        Returns dict with: symbol, bid, ask, bid_size, ask_size, exchange_ts
        or None if data is insufficient.
        """
        symbol = data.get("s", "")
        if not symbol:
            return None

        try:
            result = {
                "symbol": symbol,
                "bid": Decimal(str(data["b"])),
                "ask": Decimal(str(data["a"])),
                "bid_size": Decimal(str(data["B"])),
                "ask_size": Decimal(str(data["A"])),
            }
        except (KeyError, InvalidOperation):
            return None

        ts_ms = data.get("t")
        if ts_ms:
            result["exchange_ts"] = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

        return result
