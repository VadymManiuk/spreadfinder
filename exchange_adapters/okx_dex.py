"""
OKX DEX Market API polling adapter.

Inputs: Allowed futures base set, OKX DEX API credentials, and snapshot callback.
Outputs: Normalized DEX-style MarketSnapshot objects via callback.
Assumptions:
  - Polls OKX's token top-list endpoint sorted by 24h trading volume.
  - Uses authenticated Onchain OS / DEX API requests.
  - Emits only tokens whose normalized base exists on at least one futures venue.
  - Skips ambiguous tickers listed in TICKER_COLLISIONS to avoid false matches.
"""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from urllib.parse import urlencode

import aiohttp
import structlog

from exchange_adapters.base import BaseExchangeAdapter, SnapshotCallback
from exchange_adapters.dex_common import estimate_size_from_liquidity
from models.snapshot import MarketSnapshot
from symbol_mapper.ticker_aliases import TICKER_COLLISIONS, normalize_base
from utils.okx_auth import okx_headers

logger = structlog.get_logger(__name__)

OKX_DEX_HOST = "https://web3.okx.com"
OKX_TOPLIST_PATH = "/api/v6/dex/market/token/toplist"


class OkxDexAdapter(BaseExchangeAdapter):
    """
    Poll OKX DEX token rankings and emit DEX snapshots for futures-listed bases.
    """

    def __init__(
        self,
        allowed_bases: set[str],
        chain_indices: list[str],
        api_key: str,
        api_secret: str,
        passphrase: str,
        on_snapshot: SnapshotCallback,
        project_id: str = "",
        poll_interval_seconds: float = 30.0,
        stale_threshold_seconds: float = 90.0,
    ):
        super().__init__(
            exchange_name="okx_dex",
            on_snapshot=on_snapshot,
            stale_threshold_seconds=stale_threshold_seconds,
        )
        self._allowed_bases = set(allowed_bases)
        self._chain_indices = [chain for chain in chain_indices if chain]
        self._api_key = api_key
        self._api_secret = api_secret
        self._passphrase = passphrase
        self._project_id = project_id
        self._poll_interval_seconds = poll_interval_seconds
        self._http_session: aiohttp.ClientSession | None = None

    async def _connect(self) -> None:
        self._http_session = aiohttp.ClientSession()
        self._log.info(
            "connecting",
            chains=self._chain_indices,
            allowed_bases=len(self._allowed_bases),
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
        if not self._http_session or not self._chain_indices:
            return

        query = urlencode(
            {
                "chains": ",".join(self._chain_indices),
                "sortBy": "5",
                "timeFrame": "4",
            }
        )
        request_path = f"{OKX_TOPLIST_PATH}?{query}"
        headers = okx_headers(
            api_key=self._api_key,
            api_secret=self._api_secret,
            passphrase=self._passphrase,
            method="GET",
            request_path=request_path,
            project_id=self._project_id,
        )
        url = f"{OKX_DEX_HOST}{request_path}"

        async with self._http_session.get(url, headers=headers) as resp:
            resp.raise_for_status()
            payload = await resp.json()

        if payload.get("code") != "0":
            self._log.warning(
                "okx_dex_api_error",
                code=payload.get("code"),
                msg=payload.get("msg"),
            )
            return

        emitted = 0
        for token in payload.get("data", []):
            snapshot = self._build_snapshot(token)
            if snapshot is None:
                continue
            await self.on_snapshot(snapshot)
            emitted += 1

        self._update_heartbeat()
        self._log.debug("okx_dex_polled", emitted=emitted)

    def _build_snapshot(self, token: dict) -> MarketSnapshot | None:
        """
        Convert one OKX DEX ranking entry into a MarketSnapshot.
        """
        if not isinstance(token, dict):
            return None

        base = str(token.get("tokenSymbol", "")).strip().upper()
        if not base:
            return None

        normalized_base = normalize_base(base)
        if normalized_base in TICKER_COLLISIONS:
            return None
        if self._allowed_bases and normalized_base not in self._allowed_bases:
            return None

        try:
            price = Decimal(str(token["price"]))
            if price <= 0:
                return None
            volume_24h = Decimal(str(token["volume"]))
            liquidity_raw = token.get("liquidity")
            liquidity = (
                Decimal(str(liquidity_raw))
                if liquidity_raw not in (None, "")
                else None
            )
        except (KeyError, InvalidOperation):
            logger.debug("okx_dex_token_skipped", reason="bad_numeric_fields")
            return None

        chain_index = str(token.get("chainIndex", "")).strip() or "unknown"
        exchange = f"okx_dex:{chain_index}"
        size = estimate_size_from_liquidity(price, liquidity)

        return MarketSnapshot(
            canonical_symbol=f"{base}-{chain_index}-DEX",
            exchange=exchange,
            bid=price,
            ask=price,
            bid_size=size,
            ask_size=size,
            exchange_ts=None,
            local_ts=datetime.now(timezone.utc),
            volume_24h=volume_24h,
            is_stale=False,
        )
