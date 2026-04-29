"""
Binance Spot WebSocket adapter.

Inputs: List of Binance spot symbols to subscribe to, snapshot callback.
Outputs: Normalized MarketSnapshot objects via callback.
Assumptions:
  - Uses Binance spot combined streams.
  - Subscribes to @bookTicker for best bid/ask.
  - Spot snapshots use canonical symbols ending in SPOT and exchange "binance_spot".
"""

import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import structlog
import websockets

from exchange_adapters.base import BaseExchangeAdapter, SnapshotCallback
from models.snapshot import MarketSnapshot

logger = structlog.get_logger(__name__)

WS_BASE = "wss://stream.binance.com:9443"
MAX_STREAMS_PER_CONN = 200
PING_INTERVAL_SECONDS = 15 * 60
OPEN_TIMEOUT_SECONDS = 20


class BinanceSpotAdapter(BaseExchangeAdapter):
    """
    Binance spot WebSocket adapter.

    Subscribes to @bookTicker and emits snapshots with no funding fields.
    One stream per symbol; symbols are split across combined-stream connections
    to stay within Binance limits.
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
            symbols: Native Binance spot symbols (e.g. ["AIUSDT", "BTCUSDT"]).
            on_snapshot: Async callback receiving MarketSnapshot objects.
            canonical_map: Optional {native: canonical} mapping.
            stale_threshold_seconds: Seconds without data before marking feed stale.
        """
        super().__init__(
            exchange_name="binance_spot",
            on_snapshot=on_snapshot,
            stale_threshold_seconds=stale_threshold_seconds,
        )
        self._symbols = symbols
        self._canonical_map = canonical_map or {}
        self._symbol_chunks: list[list[str]] = [
            symbols[i:i + MAX_STREAMS_PER_CONN]
            for i in range(0, len(symbols), MAX_STREAMS_PER_CONN)
        ]
        self._ws_connections: list[websockets.WebSocketClientProtocol | None] = [
            None for _ in self._symbol_chunks
        ]
        self._ping_tasks: list[asyncio.Task | None] = [
            None for _ in self._symbol_chunks
        ]

        self._log.info(
            "binance_spot_adapter_init",
            total_symbols=len(symbols),
            connections_needed=len(self._symbol_chunks),
            symbols_per_conn=[len(c) for c in self._symbol_chunks],
        )

    @staticmethod
    def _build_ws_url_for_symbols(symbols: list[str]) -> str:
        """Build combined stream URL for a chunk of spot symbols."""
        streams = [f"{sym.lower()}@bookTicker" for sym in symbols]
        return f"{WS_BASE}/stream?streams={'/'.join(streams)}"

    def _build_ws_url(self) -> str:
        """Build URL for the first chunk."""
        return self._build_ws_url_for_symbols(self._symbol_chunks[0]) if self._symbol_chunks else ""

    async def _connect(self) -> None:
        """Establish WebSocket connections to Binance spot."""
        for i, chunk in enumerate(self._symbol_chunks):
            url = self._build_ws_url_for_symbols(chunk)
            self._log.info(
                "connecting",
                connection=f"{i + 1}/{len(self._symbol_chunks)}",
                symbol_count=len(chunk),
            )
            ws = await websockets.connect(
                url,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=5,
                open_timeout=OPEN_TIMEOUT_SECONDS,
            )
            self._ws_connections[i] = ws
            self._log.info("connected", connection=f"{i + 1}/{len(self._symbol_chunks)}")

    async def _disconnect(self) -> None:
        """Close all WebSocket connections."""
        ping_tasks = [task for task in self._ping_tasks if task is not None]
        await self._cancel_tasks(ping_tasks)
        self._ping_tasks = [None for _ in self._ping_tasks]
        for i, ws in enumerate(self._ws_connections):
            if ws:
                await ws.close()
                self._ws_connections[i] = None

    async def _subscribe(self) -> None:
        """Start ping keep-alive tasks; URL already contains subscriptions."""
        for i in range(len(self._ws_connections)):
            self._ping_tasks[i] = asyncio.create_task(self._ping_loop(i))

    async def _ping_loop(self, conn_index: int) -> None:
        """Send periodic pings to keep a connection alive."""
        while self._running:
            await asyncio.sleep(PING_INTERVAL_SECONDS)
            ws = self._ws_connections[conn_index]
            if ws:
                try:
                    await ws.ping()
                    self._log.debug("ping_sent", connection=conn_index)
                except Exception:
                    self._log.warning("ping_failed", connection=conn_index, exc_info=True)
                    return

    async def _listen(self) -> None:
        """Receive and process messages from all connections concurrently."""
        tasks = [
            asyncio.create_task(self._listen_one(i))
            for i in range(len(self._ws_connections))
        ]
        error = await self._wait_until_first_task_finishes(tasks)
        if error:
            raise error

    async def _listen_one(self, conn_index: int) -> None:
        """Receive and process messages from a single connection."""
        ws = self._ws_connections[conn_index]
        if not ws:
            return

        async for raw_message in ws:
            self._update_heartbeat()
            try:
                message = json.loads(raw_message)
                data = message.get("data", {})
                await self._handle_book_ticker(data)
            except (json.JSONDecodeError, InvalidOperation, KeyError):
                self._log.warning("parse_error", raw=str(raw_message)[:200])
            except Exception:
                self._log.exception("message_handler_error")

    async def _handle_book_ticker(self, data: dict) -> None:
        """
        Process @bookTicker message and emit snapshot.

        Payload:
          {"u": 1, "s": "AIUSDT", "b": "0.1200", "B": "100",
           "a": "0.1210", "A": "90"}
        """
        parsed = self.parse_book_ticker(data)
        native_symbol = parsed["symbol"]
        canonical = self._canonical_map.get(native_symbol, native_symbol)

        snapshot = MarketSnapshot(
            canonical_symbol=canonical,
            exchange="binance_spot",
            bid=parsed["bid"],
            ask=parsed["ask"],
            bid_size=parsed["bid_size"],
            ask_size=parsed["ask_size"],
            exchange_ts=parsed.get("exchange_ts"),
            local_ts=datetime.now(timezone.utc),
            is_stale=False,
        )

        await self.on_snapshot(snapshot)

    @staticmethod
    def parse_book_ticker(data: dict) -> dict:
        """
        Parse a raw spot bookTicker payload into typed values.

        Returns dict with: symbol, bid, ask, bid_size, ask_size.
        """
        result = {
            "symbol": data.get("s", ""),
            "bid": Decimal(data["b"]),
            "ask": Decimal(data["a"]),
            "bid_size": Decimal(data["B"]),
            "ask_size": Decimal(data["A"]),
        }
        event_ts_ms = data.get("E")
        if event_ts_ms:
            result["exchange_ts"] = datetime.fromtimestamp(
                int(event_ts_ms) / 1000,
                tz=timezone.utc,
            )
        return result
