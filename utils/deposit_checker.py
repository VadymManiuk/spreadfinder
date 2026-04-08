"""
Deposit/withdrawal availability checker for exchange tokens.

Inputs: Exchange name, base token ticker.
Outputs: {"deposit": bool|None, "withdraw": bool|None} — None = unknown.
Assumptions:
  - Gate and Bitget have public (no API key) endpoints for coin status.
  - DEXes (Hyperliquid, Aster, Lighter) are always available (bridge-based).
  - Binance, Bybit, OKX need API keys — returns None (unknown) for these.
  - Refreshed every 5 minutes to catch chain suspensions.
"""

import asyncio
from dataclasses import dataclass

import aiohttp
import structlog

logger = structlog.get_logger(__name__)

# DEXes — deposits/withdrawals are always available (on-chain bridging)
DEX_EXCHANGES = {"hyperliquid", "aster", "lighter"}

# Refresh interval for deposit/withdrawal status
REFRESH_INTERVAL_SECONDS = 300  # 5 minutes


@dataclass
class CoinStatus:
    deposit: bool | None  # True=enabled, False=disabled, None=unknown
    withdraw: bool | None

    @property
    def deposit_symbol(self) -> str:
        if self.deposit is True:
            return "✅"
        elif self.deposit is False:
            return "❌"
        return "⚪"

    @property
    def withdraw_symbol(self) -> str:
        if self.withdraw is True:
            return "✅"
        elif self.withdraw is False:
            return "❌"
        return "⚪"

    def format_short(self) -> str:
        """Compact format: '✅D ✅W' or '❌D ✅W' or '⚪'."""
        if self.deposit is None and self.withdraw is None:
            return "⚪"
        return f"{self.deposit_symbol}D {self.withdraw_symbol}W"


UNKNOWN = CoinStatus(deposit=None, withdraw=None)
AVAILABLE = CoinStatus(deposit=True, withdraw=True)


class DepositChecker:
    """
    Periodically fetches deposit/withdrawal status from exchanges
    with public APIs. Provides O(1) lookup by (exchange, base_token).
    """

    def __init__(self) -> None:
        # (exchange, BASE) -> CoinStatus
        self._status: dict[tuple[str, str], CoinStatus] = {}
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """Fetch initial data and start periodic refresh."""
        await self._refresh()
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def get_status(self, exchange: str, base: str) -> CoinStatus:
        """Get deposit/withdrawal status for a token on an exchange."""
        if exchange in DEX_EXCHANGES:
            return AVAILABLE
        return self._status.get((exchange, base.upper()), UNKNOWN)

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(REFRESH_INTERVAL_SECONDS)
            try:
                await self._refresh()
            except Exception:
                logger.exception("deposit_checker_refresh_error")

    async def _refresh(self) -> None:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            results = await asyncio.gather(
                self._fetch_gate(session),
                self._fetch_bitget(session),
                return_exceptions=True,
            )
            for r in results:
                if isinstance(r, Exception):
                    logger.warning("deposit_fetch_error", error=str(r))

        logger.info("deposit_status_refreshed", total_coins=len(self._status))

    async def _fetch_gate(self, session: aiohttp.ClientSession) -> None:
        """
        Gate public API: GET /api/v4/spot/currencies
        Returns array of {currency, deposit_disabled, withdraw_disabled, ...}
        """
        url = "https://api.gateio.ws/api/v4/spot/currencies"
        async with session.get(url) as resp:
            resp.raise_for_status()
            data = await resp.json()

        count = 0
        for coin in data:
            ticker = coin.get("currency", "").upper()
            if not ticker:
                continue
            deposit_ok = not coin.get("deposit_disabled", True)
            withdraw_ok = not coin.get("withdraw_disabled", True)
            self._status[("gate", ticker)] = CoinStatus(
                deposit=deposit_ok, withdraw=withdraw_ok
            )
            count += 1

        logger.info("gate_deposit_status_fetched", coins=count)

    async def _fetch_bitget(self, session: aiohttp.ClientSession) -> None:
        """
        Bitget public API: GET /api/v2/spot/public/coins
        Returns {data: [{coin, chains: [{rechargeable, withdrawable}]}]}
        A coin is available if ANY chain supports the operation.
        """
        url = "https://api.bitget.com/api/v2/spot/public/coins"
        async with session.get(url) as resp:
            resp.raise_for_status()
            data = await resp.json()

        count = 0
        for coin_info in data.get("data", []):
            ticker = coin_info.get("coin", "").upper()
            if not ticker:
                continue
            chains = coin_info.get("chains", [])
            # Available if ANY chain supports the operation
            deposit_ok = any(
                str(c.get("rechargeable", "false")).lower() == "true"
                for c in chains
            )
            withdraw_ok = any(
                str(c.get("withdrawable", "false")).lower() == "true"
                for c in chains
            )
            self._status[("bitget", ticker)] = CoinStatus(
                deposit=deposit_ok, withdraw=withdraw_ok
            )
            count += 1

        logger.info("bitget_deposit_status_fetched", coins=count)
