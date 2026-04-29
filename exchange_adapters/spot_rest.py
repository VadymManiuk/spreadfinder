"""
Shared REST polling adapter for centralized spot markets.

Inputs: Exchange identifier, native spot symbols, canonical map, snapshot callback.
Outputs: Normalized MarketSnapshot objects via callback.
Assumptions:
  - Used for CEX spot venues where polling all tickers is simpler and safer
    than maintaining many WebSocket subscriptions.
  - Native symbols are already bootstrapped by SymbolMapper.
  - Spot snapshots use exchange names ending in "_spot".
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

import aiohttp
import structlog

from exchange_adapters.base import BaseExchangeAdapter, SnapshotCallback
from models.snapshot import MarketSnapshot

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class SpotTicker:
    """Normalized top-of-book spot ticker."""

    symbol: str
    bid: Decimal
    ask: Decimal
    bid_size: Decimal | None = None
    ask_size: Decimal | None = None
    volume_24h: Decimal | None = None
    exchange_ts: datetime | None = None


Parser = Callable[[Any], list[SpotTicker]]


def _to_decimal(value: Any) -> Decimal | None:
    """Convert an exchange value to Decimal, returning None for empty fields."""
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _ts_ms(value: Any) -> datetime | None:
    """Convert millisecond timestamp to UTC datetime."""
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def _estimate_size_from_volume(price: Decimal, volume_24h: Decimal | None) -> Decimal:
    """
    Estimate top-of-book size when an exchange ticker lacks L1 quantities.

    estimated_size = (quote_volume_24h / price) * 0.0005
    ESTIMATE — REST tickers without L1 size are less precise than WebSockets.
    """
    if price <= 0 or volume_24h is None or volume_24h <= 0:
        return Decimal("0")
    return (volume_24h / price) * Decimal("0.0005")


def _valid_ticker(
    symbol: str,
    bid: Decimal | None,
    ask: Decimal | None,
) -> bool:
    """Return True when mandatory top-of-book fields are usable."""
    return bool(symbol) and bid is not None and ask is not None and bid > 0 and ask > 0


def parse_gate_spot_tickers(payload: Any) -> list[SpotTicker]:
    """Parse Gate spot /spot/tickers response."""
    rows = payload if isinstance(payload, list) else []
    tickers: list[SpotTicker] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("currency_pair", "")).upper()
        bid = _to_decimal(row.get("highest_bid"))
        ask = _to_decimal(row.get("lowest_ask"))
        if not _valid_ticker(symbol, bid, ask):
            continue
        quote_volume = _to_decimal(row.get("quote_volume"))
        size = _estimate_size_from_volume((bid + ask) / 2, quote_volume)
        tickers.append(
            SpotTicker(
                symbol=symbol,
                bid=bid,
                ask=ask,
                bid_size=size,
                ask_size=size,
                volume_24h=quote_volume,
            )
        )
    return tickers


def parse_bybit_spot_tickers(payload: Any) -> list[SpotTicker]:
    """Parse Bybit v5 spot tickers response."""
    rows = payload.get("result", {}).get("list", []) if isinstance(payload, dict) else []
    tickers: list[SpotTicker] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).upper()
        bid = _to_decimal(row.get("bid1Price"))
        ask = _to_decimal(row.get("ask1Price"))
        if not _valid_ticker(symbol, bid, ask):
            continue
        tickers.append(
            SpotTicker(
                symbol=symbol,
                bid=bid,
                ask=ask,
                bid_size=_to_decimal(row.get("bid1Size")),
                ask_size=_to_decimal(row.get("ask1Size")),
                volume_24h=_to_decimal(row.get("turnover24h")),
            )
        )
    return tickers


def parse_okx_spot_tickers(payload: Any) -> list[SpotTicker]:
    """Parse OKX spot tickers response."""
    rows = payload.get("data", []) if isinstance(payload, dict) else []
    tickers: list[SpotTicker] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("instId", "")).upper()
        bid = _to_decimal(row.get("bidPx"))
        ask = _to_decimal(row.get("askPx"))
        if not _valid_ticker(symbol, bid, ask):
            continue
        tickers.append(
            SpotTicker(
                symbol=symbol,
                bid=bid,
                ask=ask,
                bid_size=_to_decimal(row.get("bidSz")),
                ask_size=_to_decimal(row.get("askSz")),
                volume_24h=_to_decimal(row.get("volCcy24h")),
                exchange_ts=_ts_ms(row.get("ts")),
            )
        )
    return tickers


def parse_bitget_spot_tickers(payload: Any) -> list[SpotTicker]:
    """Parse Bitget spot tickers response."""
    rows = payload.get("data", []) if isinstance(payload, dict) else []
    tickers: list[SpotTicker] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).upper()
        bid = _to_decimal(row.get("bidPr"))
        ask = _to_decimal(row.get("askPr"))
        if not _valid_ticker(symbol, bid, ask):
            continue
        tickers.append(
            SpotTicker(
                symbol=symbol,
                bid=bid,
                ask=ask,
                bid_size=_to_decimal(row.get("bidSz")),
                ask_size=_to_decimal(row.get("askSz")),
                volume_24h=_to_decimal(
                    row.get("quoteVolume")
                    or row.get("usdtVolume")
                    or row.get("quoteVol")
                ),
                exchange_ts=_ts_ms(row.get("ts")),
            )
        )
    return tickers


def parse_mexc_spot_tickers(payload: Any) -> list[SpotTicker]:
    """Parse MEXC spot 24hr ticker response."""
    rows = payload if isinstance(payload, list) else []
    tickers: list[SpotTicker] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol", "")).upper()
        bid = _to_decimal(row.get("bidPrice"))
        ask = _to_decimal(row.get("askPrice"))
        if not _valid_ticker(symbol, bid, ask):
            continue
        tickers.append(
            SpotTicker(
                symbol=symbol,
                bid=bid,
                ask=ask,
                bid_size=_to_decimal(row.get("bidQty")),
                ask_size=_to_decimal(row.get("askQty")),
                volume_24h=_to_decimal(row.get("quoteVolume")),
                exchange_ts=_ts_ms(row.get("closeTime")),
            )
        )
    return tickers


SPOT_TICKER_URLS: dict[str, str] = {
    "gate_spot": "https://api.gateio.ws/api/v4/spot/tickers",
    "bybit_spot": "https://api.bybit.com/v5/market/tickers?category=spot",
    "okx_spot": "https://www.okx.com/api/v5/market/tickers?instType=SPOT",
    "bitget_spot": "https://api.bitget.com/api/v2/spot/market/tickers",
    "mexc_spot": "https://api.mexc.com/api/v3/ticker/24hr",
}

SPOT_TICKER_PARSERS: dict[str, Parser] = {
    "gate_spot": parse_gate_spot_tickers,
    "bybit_spot": parse_bybit_spot_tickers,
    "okx_spot": parse_okx_spot_tickers,
    "bitget_spot": parse_bitget_spot_tickers,
    "mexc_spot": parse_mexc_spot_tickers,
}


class SpotRestAdapter(BaseExchangeAdapter):
    """
    Poll one CEX spot ticker endpoint and emit snapshots for subscribed symbols.
    """

    def __init__(
        self,
        exchange_name: str,
        symbols: list[str],
        on_snapshot: SnapshotCallback,
        canonical_map: dict[str, str],
        poll_interval_seconds: float = 30.0,
        stale_threshold_seconds: float = 90.0,
    ):
        if exchange_name not in SPOT_TICKER_URLS:
            raise ValueError(f"unsupported spot REST exchange: {exchange_name}")

        super().__init__(
            exchange_name=exchange_name,
            on_snapshot=on_snapshot,
            stale_threshold_seconds=stale_threshold_seconds,
        )
        self._symbols = set(symbols)
        self._canonical_map = canonical_map
        self._poll_interval_seconds = poll_interval_seconds
        self._url = SPOT_TICKER_URLS[exchange_name]
        self._parser = SPOT_TICKER_PARSERS[exchange_name]
        self._http_session: aiohttp.ClientSession | None = None

    async def _connect(self) -> None:
        self._http_session = aiohttp.ClientSession()
        self._log.info(
            "connecting",
            url=self._url,
            symbol_count=len(self._symbols),
            poll_interval_seconds=self._poll_interval_seconds,
        )

    async def _disconnect(self) -> None:
        if self._http_session:
            await self._http_session.close()
            self._http_session = None

    async def _subscribe(self) -> None:
        """No-op for REST polling adapters."""

    async def _listen(self) -> None:
        if not self._http_session:
            return

        while self._running:
            await self._poll_once()
            await asyncio.sleep(self._poll_interval_seconds)

    async def _poll_once(self) -> None:
        if not self._http_session:
            return

        async with self._http_session.get(self._url) as resp:
            resp.raise_for_status()
            payload = await resp.json()

        emitted = 0
        for ticker in self._parser(payload):
            if ticker.symbol not in self._symbols:
                continue
            snapshot = self._build_snapshot(ticker)
            await self.on_snapshot(snapshot)
            emitted += 1

        self._update_heartbeat()
        self._log.debug("spot_rest_polled", emitted=emitted)

    def _build_snapshot(self, ticker: SpotTicker) -> MarketSnapshot:
        """Convert one normalized ticker into a MarketSnapshot."""
        canonical = self._canonical_map.get(ticker.symbol, ticker.symbol)
        bid_size = ticker.bid_size if ticker.bid_size is not None else Decimal("0")
        ask_size = ticker.ask_size if ticker.ask_size is not None else Decimal("0")

        return MarketSnapshot(
            canonical_symbol=canonical,
            exchange=self.exchange_name,
            bid=ticker.bid,
            ask=ticker.ask,
            bid_size=bid_size,
            ask_size=ask_size,
            exchange_ts=ticker.exchange_ts,
            local_ts=datetime.now(timezone.utc),
            volume_24h=ticker.volume_24h,
            is_stale=False,
        )
