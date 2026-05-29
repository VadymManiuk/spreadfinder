"""
Market cap filter using CoinGecko API.

Inputs: Base asset names (e.g. "APE", "BTC").
Outputs: Filtered set of symbols within the target market cap range.
Assumptions:
  - Uses CoinGecko free API (no key required, rate limited).
  - Can use CoinMarketCap first when a CMC API key is configured.
  - Target: small-cap tokens under $200M market cap.
  - Tokens above $1B are always excluded (spreads too tight).
  - Unknown market cap tokens are INCLUDED (conservative — don't miss opportunities).
  - Market cap data is refreshed periodically (default: every 30 min).
"""

import asyncio
import time

import aiohttp
import structlog

logger = structlog.get_logger(__name__)

# CoinGecko free API — no key needed, 10-30 req/min rate limit
COINGECKO_API = "https://api.coingecko.com/api/v3"
COINMARKETCAP_API = "https://pro-api.coinmarketcap.com/v2"

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
        coinmarketcap_api_key: str = "",
        coingecko_api_key: str = "",
        on_demand_miss_ttl_seconds: int = 30 * 60,
    ):
        self.max_mcap = max_mcap
        self.min_mcap = min_mcap
        self.refresh_interval = refresh_interval
        self.coinmarketcap_api_key = coinmarketcap_api_key
        self.coingecko_api_key = coingecko_api_key
        self.on_demand_miss_ttl_seconds = on_demand_miss_ttl_seconds

        # {symbol_upper: market_cap_usd} — populated by refresh()
        self._mcap_data: dict[str, float] = {}
        self._mcap_miss_ts: dict[str, float] = {}

        # Manual overrides: symbols to always include or exclude
        self._always_include: set[str] = set()
        self._always_exclude: set[str] = set()

        self._session: aiohttp.ClientSession | None = None
        self._refresh_task: asyncio.Task | None = None
        self._lookup_lock = asyncio.Lock()

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
                    headers=self._coingecko_headers(),
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
        self._mcap_miss_ts.clear()
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

    async def get_mcap_async(self, base_symbol: str) -> float | None:
        """
        Get market cap from cache, then try an on-demand provider lookup.

        The periodic CoinGecko refresh only covers the top pages by market cap,
        which misses many long-tail futures symbols. This lookup fills that gap
        just before alert formatting.
        """
        symbol = base_symbol.upper()
        cached = self._mcap_data.get(symbol)
        if cached is not None:
            return cached

        miss_ts = self._mcap_miss_ts.get(symbol)
        if miss_ts is not None and time.monotonic() - miss_ts < self.on_demand_miss_ttl_seconds:
            return None

        async with self._lookup_lock:
            cached = self._mcap_data.get(symbol)
            if cached is not None:
                return cached

            mcap = await self._fetch_mcap_on_demand(symbol)
            if mcap is None:
                self._mcap_miss_ts[symbol] = time.monotonic()
                return None

            self._mcap_data[symbol] = mcap
            self._mcap_miss_ts.pop(symbol, None)
            return mcap

    async def _fetch_mcap_on_demand(self, symbol: str) -> float | None:
        """Fetch market cap for one symbol from CMC, then CoinGecko."""
        if self._session is None:
            async with aiohttp.ClientSession() as session:
                return await self._fetch_mcap_with_session(session, symbol)
        return await self._fetch_mcap_with_session(self._session, symbol)

    async def _fetch_mcap_with_session(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
    ) -> float | None:
        """Fetch market cap for one symbol using the configured providers."""
        mcap = await self._fetch_coinmarketcap_mcap(session, symbol)
        if mcap is not None:
            return mcap
        return await self._fetch_coingecko_mcap(session, symbol)

    async def _fetch_coinmarketcap_mcap(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
    ) -> float | None:
        """Fetch one symbol's market cap from CoinMarketCap when configured."""
        if not self.coinmarketcap_api_key:
            return None

        try:
            async with session.get(
                f"{COINMARKETCAP_API}/cryptocurrency/quotes/latest",
                params={"symbol": symbol, "convert": "USD"},
                headers={"X-CMC_PRO_API_KEY": self.coinmarketcap_api_key},
            ) as resp:
                if resp.status == 429:
                    logger.warning("coinmarketcap_rate_limited", symbol=symbol)
                    return None
                if resp.status in (401, 403):
                    logger.warning("coinmarketcap_auth_failed", status=resp.status)
                    return None
                resp.raise_for_status()
                payload = await resp.json()
        except aiohttp.ClientError:
            logger.exception("coinmarketcap_mcap_lookup_error", symbol=symbol)
            return None

        entries = payload.get("data", {}).get(symbol)
        if isinstance(entries, dict):
            entries = [entries]
        if not isinstance(entries, list):
            return None

        for entry in entries:
            quote = entry.get("quote", {}).get("USD", {})
            mcap = quote.get("market_cap")
            if mcap is not None:
                return float(mcap)
        return None

    async def _fetch_coingecko_mcap(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
    ) -> float | None:
        """Fetch one symbol's market cap from CoinGecko search + markets."""
        try:
            async with session.get(
                f"{COINGECKO_API}/search",
                params={"query": symbol},
                headers=self._coingecko_headers(),
            ) as resp:
                if resp.status == 429:
                    logger.warning("coingecko_rate_limited", symbol=symbol, endpoint="search")
                    return None
                resp.raise_for_status()
                search_payload = await resp.json()
        except aiohttp.ClientError:
            logger.exception("coingecko_search_error", symbol=symbol)
            return None

        candidates = [
            coin
            for coin in search_payload.get("coins", [])
            if coin.get("symbol", "").upper() == symbol
        ]
        candidates.sort(key=lambda c: c.get("market_cap_rank") or 10**9)
        ids = [coin.get("id") for coin in candidates[:10] if coin.get("id")]
        if not ids:
            return None

        try:
            async with session.get(
                f"{COINGECKO_API}/coins/markets",
                params={
                    "vs_currency": "usd",
                    "ids": ",".join(ids),
                    "order": "market_cap_desc",
                    "per_page": len(ids),
                    "page": 1,
                    "sparkline": "false",
                },
                headers=self._coingecko_headers(),
            ) as resp:
                if resp.status == 429:
                    logger.warning("coingecko_rate_limited", symbol=symbol, endpoint="markets")
                    return None
                resp.raise_for_status()
                markets = await resp.json()
        except aiohttp.ClientError:
            logger.exception("coingecko_market_lookup_error", symbol=symbol)
            return None

        by_id = {coin.get("id"): coin for coin in markets}
        for coin_id in ids:
            mcap = by_id.get(coin_id, {}).get("market_cap")
            if mcap is not None:
                return float(mcap)
        return None

    def _coingecko_headers(self) -> dict[str, str]:
        """Return CoinGecko headers, including optional demo API key."""
        if not self.coingecko_api_key:
            return {}
        return {"x-cg-demo-api-key": self.coingecko_api_key}

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
