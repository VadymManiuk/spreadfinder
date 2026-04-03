"""
Market cap filter using CoinGecko API.

Inputs: Base asset names (e.g. "APE", "BTC").
Outputs: Filtered set of symbols within the target market cap range.
Assumptions:
  - Uses CoinGecko free API (no key required, rate limited).
  - Target: small-cap tokens under $200M market cap.
  - Tokens above $1B are always excluded (spreads too tight).
  - Unknown market cap tokens are INCLUDED (conservative — don't miss opportunities).
  - Market cap data is refreshed periodically (default: every 30 min).
"""

import asyncio
from decimal import Decimal

import aiohttp
import structlog

logger = structlog.get_logger(__name__)

# CoinGecko free API — no key needed, 10-30 req/min rate limit
COINGECKO_API = "https://api.coingecko.com/api/v3"

# Default thresholds for small-cap focus
DEFAULT_MAX_MCAP = 200_000_000    # $200M — above this, skip
DEFAULT_MIN_MCAP = 0              # no floor
HARD_EXCLUDE_MCAP = 1_000_000_000 # $1B — always exclude regardless of config

# How often to refresh market cap data (seconds)
DEFAULT_REFRESH_INTERVAL = 30 * 60  # 30 minutes


class MarketCapFilter:
    """
    Filters tokens by market cap using CoinGecko data.

    Usage:
        mcap_filter = MarketCapFilter(max_mcap=200_000_000)
        await mcap_filter.refresh()
        if mcap_filter.is_allowed("APE"):
            ...  # process this token
    """

    def __init__(
        self,
        max_mcap: int = DEFAULT_MAX_MCAP,
        min_mcap: int = DEFAULT_MIN_MCAP,
        refresh_interval: int = DEFAULT_REFRESH_INTERVAL,
    ):
        self.max_mcap = max_mcap
        self.min_mcap = min_mcap
        self.refresh_interval = refresh_interval

        # {symbol_upper: market_cap_usd} — populated by refresh()
        self._mcap_data: dict[str, float] = {}

        # Manual overrides: symbols to always include or exclude
        self._always_include: set[str] = set()
        self._always_exclude: set[str] = set()

        self._session: aiohttp.ClientSession | None = None
        self._refresh_task: asyncio.Task | None = None

    async def start(self, session: aiohttp.ClientSession | None = None) -> None:
        """Start the filter: fetch initial data and begin periodic refresh."""
        self._session = session or aiohttp.ClientSession()
        await self.refresh()
        self._refresh_task = asyncio.create_task(self._refresh_loop())

    async def stop(self) -> None:
        """Stop periodic refresh."""
        if self._refresh_task:
            self._refresh_task.cancel()
            self._refresh_task = None
        if self._session:
            await self._session.close()
            self._session = None

    async def _refresh_loop(self) -> None:
        """Periodically refresh market cap data."""
        while True:
            await asyncio.sleep(self.refresh_interval)
            try:
                await self.refresh()
            except Exception:
                logger.exception("mcap_refresh_error")

    async def refresh(self) -> None:
        """
        Fetch market cap data from CoinGecko.

        Uses /coins/markets endpoint which returns top coins by market cap.
        We fetch multiple pages to cover small caps.
        """
        if not self._session:
            return

        all_coins: dict[str, float] = {}

        # Fetch up to 5 pages (250 coins per page = 1250 coins)
        # This covers most listed perp futures assets
        for page in range(1, 6):
            try:
                async with self._session.get(
                    f"{COINGECKO_API}/coins/markets",
                    params={
                        "vs_currency": "usd",
                        "order": "market_cap_desc",
                        "per_page": 250,
                        "page": page,
                        "sparkline": "false",
                    },
                ) as resp:
                    if resp.status == 429:
                        logger.warning("coingecko_rate_limited", page=page)
                        await asyncio.sleep(60)
                        break
                    resp.raise_for_status()
                    coins = await resp.json()

                if not coins:
                    break

                for coin in coins:
                    symbol = coin.get("symbol", "").upper()
                    mcap = coin.get("market_cap")
                    if symbol and mcap is not None:
                        all_coins[symbol] = float(mcap)

                # Rate limit: be polite to free API
                await asyncio.sleep(2)

            except aiohttp.ClientError:
                logger.exception("coingecko_fetch_error", page=page)
                break

        self._mcap_data = all_coins
        logger.info(
            "mcap_data_refreshed",
            total_coins=len(all_coins),
            sample_small_caps=[
                s for s, m in sorted(all_coins.items(), key=lambda x: x[1])
                if m < self.max_mcap
            ][:10],
        )

    def is_allowed(self, base_symbol: str) -> bool:
        """
        Check if a token is within the allowed market cap range.

        Rules:
          1. Always-include overrides everything.
          2. Always-exclude overrides market cap check.
          3. Above $1B hard ceiling → always excluded.
          4. Above max_mcap → excluded.
          5. Below min_mcap → excluded.
          6. Unknown market cap → INCLUDED (don't miss opportunities).
        """
        symbol = base_symbol.upper()

        if symbol in self._always_include:
            return True
        if symbol in self._always_exclude:
            return False

        mcap = self._mcap_data.get(symbol)

        if mcap is None:
            # Unknown market cap — include conservatively
            logger.debug("mcap_unknown_included", symbol=symbol)
            return True

        if mcap >= HARD_EXCLUDE_MCAP:
            return False
        if mcap > self.max_mcap:
            return False
        if mcap < self.min_mcap:
            return False

        return True

    def get_mcap(self, base_symbol: str) -> float | None:
        """Get cached market cap for a symbol, or None if unknown."""
        return self._mcap_data.get(base_symbol.upper())

    def filter_symbols(self, base_symbols: list[str]) -> list[str]:
        """Filter a list of base symbols, returning only allowed ones."""
        return [s for s in base_symbols if self.is_allowed(s)]

    def load_static(self, data: dict[str, float]) -> None:
        """Load static market cap data for testing."""
        self._mcap_data = {k.upper(): v for k, v in data.items()}

    def add_always_include(self, symbols: list[str]) -> None:
        """Add symbols that should always pass the filter."""
        self._always_include.update(s.upper() for s in symbols)

    def add_always_exclude(self, symbols: list[str]) -> None:
        """Add symbols that should always be rejected."""
        self._always_exclude.update(s.upper() for s in symbols)
