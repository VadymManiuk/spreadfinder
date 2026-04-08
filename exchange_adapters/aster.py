"""
Aster DEX Perpetual Futures WebSocket adapter.

Inputs: List of symbols to subscribe to, snapshot callback.
Outputs: Normalized MarketSnapshot objects via callback.
Assumptions:
  - Aster API is a Binance Futures API clone — identical WS/REST format.
  - Uses combined stream URL: wss://fstream.asterdex.com/stream?streams=...
  - Subscribes to @bookTicker (best bid/ask) and @markPrice (mark, index, funding).
  - Uses the same 200-stream cap per connection as Binance Futures.
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

WS_BASE = "wss://fstream.asterdex.com"
MAX_STREAMS_PER_CONN = 200
PING_INTERVAL_SECONDS = 15 * 60
OPEN_TIMEOUT_SECONDS = 20


class AsterAdapter(BaseExchangeAdapter):
    """
    Aster DEX Perpetual Futures adapter.
    Identical to Binance adapter — same WS protocol, same message format.
    """

    def __init__(
        self,
        symbols: list[str],
        on_snapshot: SnapshotCallback,
        canonical_map: dict[str, str] | None = None,
        stale_threshold_seconds: float = 10.0,
    ):
        super().__init__(
            exchange_name="aster",
            on_snapshot=on_snapshot,
            stale_threshold_seconds=stale_threshold_seconds,
        )
        self._symbols = symbols
        self._canonical_map = canonical_map or {}

        max_symbols_per_conn = MAX_STREAMS_PER_CONN // 2
        self._symbol_chunks: list[list[str]] = [
            symbols[i:i + max_symbols_per_conn]
            for i in range(0, len(symbols), max_symbols_per_conn)
        ]
        self._ws_connections: list[websockets.WebSocketClientProtocol | None] = [
            None for _ in self._symbol_chunks
        ]
        self._ping_tasks: list[asyncio.Task | None] = [
            None for _ in self._symbol_chunks
        ]
        self._state: dict[str, dict] = {}

        self._log.info(
            "aster_adapter_init",
            total_symbols=len(symbols),
            connections_needed=len(self._symbol_chunks),
            symbols_per_conn=[len(c) for c in self._symbol_chunks],
        )

    @staticmethod
    def _build_ws_url_for_symbols(symbols: list[str]) -> str:
        streams = []
        for sym in symbols:
            lower = sym.lower()
            streams.append(f"{lower}@bookTicker")
            streams.append(f"{lower}@markPrice")
        return f"{WS_BASE}/stream?streams={'/'.join(streams)}"

    async def _connect(self) -> None:
        for i, chunk in enumerate(self._symbol_chunks):
            url = self._build_ws_url_for_symbols(chunk)
            self._log.info(
                "connecting",
                connection=f"{i+1}/{len(self._symbol_chunks)}",
                symbol_count=len(chunk),
                stream_count=len(chunk) * 2,
            )
            ws = await websockets.connect(
                url,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=5,
                open_timeout=OPEN_TIMEOUT_SECONDS,
            )
            self._ws_connections[i] = ws
            self._log.info("connected", connection=f"{i+1}/{len(self._symbol_chunks)}")

    async def _disconnect(self) -> None:
        ping_tasks = [task for task in self._ping_tasks if task is not None]
        await self._cancel_tasks(ping_tasks)
        self._ping_tasks = [None for _ in self._ping_tasks]
        for i, ws in enumerate(self._ws_connections):
            if ws:
                await ws.close()
                self._ws_connections[i] = None

    async def _subscribe(self) -> None:
        for i in range(len(self._ws_connections)):
            self._ping_tasks[i] = asyncio.create_task(self._ping_loop(i))

    async def _ping_loop(self, conn_index: int) -> None:
        while self._running:
            await asyncio.sleep(PING_INTERVAL_SECONDS)
            ws = self._ws_connections[conn_index]
            if ws:
                try:
                    await ws.ping()
                except Exception:
                    self._log.warning("ping_failed", connection=conn_index)
                    return

    async def _listen(self) -> None:
        tasks = [asyncio.create_task(self._listen_one(i)) for i in range(len(self._ws_connections))]
        error = await self._wait_until_first_task_finishes(tasks)
        if error:
            raise error

    async def _listen_one(self, conn_index: int) -> None:
        ws = self._ws_connections[conn_index]
        if not ws:
            return
        async for raw in ws:
            self._update_heartbeat()
            try:
                message = json.loads(raw)
                stream = message.get("stream", "")
                data = message.get("data", {})
                if stream.endswith("@bookTicker"):
                    await self._handle_book_ticker(data)
                elif stream.endswith("@markPrice"):
                    await self._handle_mark_price(data)
            except (json.JSONDecodeError, InvalidOperation):
                self._log.warning("parse_error", raw=str(raw)[:200])
            except Exception:
                self._log.exception("message_handler_error")

    async def _handle_book_ticker(self, data: dict) -> None:
        symbol = data.get("s", "")
        if symbol not in self._state:
            self._state[symbol] = {}
        state = self._state[symbol]
        state["bid"] = Decimal(data["b"])
        state["ask"] = Decimal(data["a"])
        state["bid_size"] = Decimal(data["B"])
        state["ask_size"] = Decimal(data["A"])
        ts_ms = data.get("T")
        if ts_ms:
            state["exchange_ts"] = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        await self._emit_snapshot(symbol)

    async def _handle_mark_price(self, data: dict) -> None:
        symbol = data.get("s", "")
        if symbol not in self._state:
            self._state[symbol] = {}
        state = self._state[symbol]
        state["mark_price"] = Decimal(data.get("p", "0"))
        state["index_price"] = Decimal(data.get("i", "0"))
        state["funding_rate"] = Decimal(data.get("r", "0"))

        # T = next funding settlement time (ms since epoch), same as Binance
        nft = data.get("T")
        if nft:
            state["next_funding_time"] = datetime.fromtimestamp(int(nft) / 1000, tz=timezone.utc)

    async def _emit_snapshot(self, native_symbol: str) -> None:
        state = self._state.get(native_symbol, {})
        bid = state.get("bid")
        ask = state.get("ask")
        if bid is None or ask is None:
            return
        canonical = self._canonical_map.get(native_symbol, native_symbol)
        snapshot = MarketSnapshot(
            canonical_symbol=canonical,
            exchange="aster",
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
