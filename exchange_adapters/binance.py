"""
Binance Futures WebSocket adapter.

Inputs: List of symbols to subscribe to, snapshot callback.
Outputs: Normalized MarketSnapshot objects via callback.
Assumptions:
  - Uses USDT-margined perpetual futures (fapi).
  - Subscribes to @bookTicker (best bid/ask) and @markPrice (mark, index, funding).
  - Respects rate limits: max 300 streams per connection, ping every 20 min.
  - REST bootstrap for exchange info is handled by SymbolMapper, not here.

Rate limits (Binance WebSocket):
  - Max 300 streams per single connection
  - Server sends ping every 3 min; client must respond with pong
  - Client should send ping if no data for 20 min (keep-alive)
  - Max 5 messages/sec sent by client
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

# Binance WS base URL for futures
WS_BASE = "wss://fstream.binance.com"

# Max streams per connection (Binance limit: 300)
MAX_STREAMS_PER_CONN = 300

# Ping interval to keep connection alive (Binance recommends within 20 min)
PING_INTERVAL_SECONDS = 15 * 60  # 15 min, well under the 20 min limit


class BinanceAdapter(BaseExchangeAdapter):
    """
    Binance USDT-M Futures WebSocket adapter.

    Subscribes to @bookTicker and @markPrice streams for each symbol.
    Merges data into MarketSnapshot objects.

    Two streams per symbol means N symbols = 2N streams.
    With 300 stream limit, max ~150 symbols per connection.
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
            symbols: Native Binance symbols (e.g. ["BTCUSDT", "ETHUSDT"]).
            on_snapshot: Async callback receiving MarketSnapshot objects.
            canonical_map: Optional {native: canonical} mapping. If not provided,
                          symbols are used as-is (not recommended for production).
            stale_threshold_seconds: Seconds without data before marking feed stale.
        """
        super().__init__(
            exchange_name="binance",
            on_snapshot=on_snapshot,
            stale_threshold_seconds=stale_threshold_seconds,
        )
        self._symbols = symbols
        self._canonical_map = canonical_map or {}
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._ping_task: asyncio.Task | None = None

        # In-memory state: latest data per symbol, merged from multiple streams
        # {native_symbol: {field: value}}
        self._state: dict[str, dict] = {}

        # Validate stream count
        stream_count = len(symbols) * 2  # bookTicker + markPrice per symbol
        if stream_count > MAX_STREAMS_PER_CONN:
            self._log.warning(
                "too_many_streams",
                stream_count=stream_count,
                max=MAX_STREAMS_PER_CONN,
                hint="Split symbols across multiple connections",
            )

    def _build_ws_url(self) -> str:
        """
        Build combined stream URL for all symbols.

        Format: wss://fstream.binance.com/stream?streams=btcusdt@bookTicker/btcusdt@markPrice/...
        """
        streams = []
        for sym in self._symbols:
            lower = sym.lower()
            streams.append(f"{lower}@bookTicker")
            streams.append(f"{lower}@markPrice")
        return f"{WS_BASE}/stream?streams={'/'.join(streams)}"

    async def _connect(self) -> None:
        """Establish WebSocket connection to Binance combined stream."""
        url = self._build_ws_url()
        self._log.info("connecting", url=url[:80] + "...", symbol_count=len(self._symbols))
        self._ws = await websockets.connect(
            url,
            ping_interval=None,  # we handle pings ourselves
            ping_timeout=None,
            close_timeout=5,
        )
        self._log.info("connected")

    async def _disconnect(self) -> None:
        """Close WebSocket connection."""
        if self._ping_task:
            self._ping_task.cancel()
            self._ping_task = None
        if self._ws:
            await self._ws.close()
            self._ws = None

    async def _subscribe(self) -> None:
        """
        No explicit subscribe needed — Binance combined stream URL acts as subscription.
        Start the ping keep-alive task.
        """
        self._ping_task = asyncio.create_task(self._ping_loop())

    async def _ping_loop(self) -> None:
        """Send periodic pings to keep the connection alive."""
        while self._running:
            await asyncio.sleep(PING_INTERVAL_SECONDS)
            if self._ws:
                try:
                    await self._ws.ping()
                    self._log.debug("ping_sent")
                except Exception:
                    self._log.warning("ping_failed", exc_info=True)
                    return  # trigger reconnect

    async def _listen(self) -> None:
        """Receive and process messages from the combined stream."""
        if not self._ws:
            return

        async for raw_message in self._ws:
            self._update_heartbeat()
            try:
                message = json.loads(raw_message)
                stream = message.get("stream", "")
                data = message.get("data", {})

                if stream.endswith("@bookTicker"):
                    await self._handle_book_ticker(data)
                elif stream.endswith("@markPrice"):
                    await self._handle_mark_price(data)

            except (json.JSONDecodeError, InvalidOperation):
                self._log.warning("parse_error", raw=str(raw_message)[:200])
            except Exception:
                self._log.exception("message_handler_error")

    async def _handle_book_ticker(self, data: dict) -> None:
        """
        Process @bookTicker message and emit snapshot.

        bookTicker payload:
          {"s": "BTCUSDT", "b": "50000.00", "B": "1.5", "a": "50010.00", "A": "2.0",
           "T": 1234567890123, "E": 1234567890123}
        """
        symbol = data.get("s", "")
        if symbol not in self._state:
            self._state[symbol] = {}

        state = self._state[symbol]
        state["bid"] = Decimal(data["b"])
        state["ask"] = Decimal(data["a"])
        state["bid_size"] = Decimal(data["B"])
        state["ask_size"] = Decimal(data["A"])

        # Transaction time
        ts_ms = data.get("T")
        if ts_ms:
            state["exchange_ts"] = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

        await self._emit_snapshot(symbol)

    async def _handle_mark_price(self, data: dict) -> None:
        """
        Process @markPrice message and update state.

        markPrice payload:
          {"s": "BTCUSDT", "p": "50005.00", "i": "50003.00", "r": "0.00010000",
           "T": 1234567890123, "E": 1234567890123}
        """
        symbol = data.get("s", "")
        if symbol not in self._state:
            self._state[symbol] = {}

        state = self._state[symbol]
        state["mark_price"] = Decimal(data.get("p", "0"))
        state["index_price"] = Decimal(data.get("i", "0"))
        state["funding_rate"] = Decimal(data.get("r", "0"))

        # Don't emit on markPrice alone — wait for bookTicker to provide bid/ask

    async def _emit_snapshot(self, native_symbol: str) -> None:
        """Build and emit a MarketSnapshot from the current merged state."""
        state = self._state.get(native_symbol, {})
        bid = state.get("bid")
        ask = state.get("ask")

        # Need at least bid and ask to emit
        if bid is None or ask is None:
            return

        canonical = self._canonical_map.get(native_symbol, native_symbol)

        snapshot = MarketSnapshot(
            canonical_symbol=canonical,
            exchange="binance",
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
    def parse_book_ticker(data: dict) -> dict:
        """
        Parse a raw bookTicker payload into a dict of typed values.
        Useful for testing without a live connection.

        Returns dict with: symbol, bid, ask, bid_size, ask_size, exchange_ts
        """
        result = {
            "symbol": data.get("s", ""),
            "bid": Decimal(data["b"]),
            "ask": Decimal(data["a"]),
            "bid_size": Decimal(data["B"]),
            "ask_size": Decimal(data["A"]),
        }
        ts_ms = data.get("T")
        if ts_ms:
            result["exchange_ts"] = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        return result

    @staticmethod
    def parse_mark_price(data: dict) -> dict:
        """
        Parse a raw markPrice payload into a dict of typed values.
        Useful for testing without a live connection.

        Returns dict with: symbol, mark_price, index_price, funding_rate
        """
        return {
            "symbol": data.get("s", ""),
            "mark_price": Decimal(data.get("p", "0")),
            "index_price": Decimal(data.get("i", "0")),
            "funding_rate": Decimal(data.get("r", "0")),
        }
