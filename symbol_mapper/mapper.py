"""
Cross-exchange symbol mapper.

Inputs: Native exchange symbols (e.g. "BTCUSDT", "BTC", "BTC_USDT").
Outputs: Canonical symbols ("{BASE}-{QUOTE}-PERP") and reverse lookups.
Assumptions:
  - Only perpetual futures are mapped. Dated futures and spot are excluded.
  - Ambiguous or unrecognized symbols are skipped with a warning log.
  - Symbol lists are bootstrapped from exchange REST APIs on startup.
  - Quote-equivalent stablecoins (USDT ↔ USDC) are treated as matchable
    for spread detection, since they trade near $1 parity.
"""

import asyncio

import structlog
import aiohttp

from symbol_mapper.exchange_symbols import EXCHANGE_CONFIGS, ExchangeConfig
from symbol_mapper.ticker_aliases import normalize_base, TICKER_COLLISIONS

logger = structlog.get_logger(__name__)

# Per-exchange REST bootstrap timeout. Without this, a single slow/hung
# endpoint blocks the entire startup. 15s is enough for any healthy exchange
# and short enough that a geo-blocked IP fails fast.
_BOOTSTRAP_TIMEOUT_S = 15

# Retry bootstrap on transient DNS / network failures instead of permanently
# starting the bot with a half-empty symbol map after one bad startup window.
_BOOTSTRAP_MAX_ATTEMPTS = 4
_BOOTSTRAP_RETRY_DELAY_S = 5.0


class SymbolMapper:
    """
    Maps symbols between exchange-native formats and canonical format.

    Usage:
        mapper = SymbolMapper()
        await mapper.bootstrap()  # fetches symbol lists from all exchanges
        canonical = mapper.to_canonical("binance", "BTCUSDT")
        native = mapper.to_native("gate", "BTC-USDT-PERP")
        common = mapper.get_common_symbols()
    """

    def __init__(self, exchanges: list[str] | None = None):
        """
        Args:
            exchanges: List of exchange names to load. Defaults to all configured.
        """
        self._exchange_names = exchanges or list(EXCHANGE_CONFIGS.keys())
        # exchange -> {native_symbol -> canonical_symbol}
        self._native_to_canonical: dict[str, dict[str, str]] = {}
        # exchange -> {canonical_symbol -> native_symbol}
        self._canonical_to_native: dict[str, dict[str, str]] = {}
        # exchange -> last bootstrap error message (None on success).
        # Surfaced by /status so a REST failure is visible without tailing logs.
        self._bootstrap_errors: dict[str, str | None] = {}

    async def bootstrap(self, session: aiohttp.ClientSession | None = None) -> None:
        """
        Fetch symbol lists from all configured exchanges and build lookup maps.

        Creates an aiohttp session if none is provided.
        """
        own_session = session is None
        if own_session:
            session = aiohttp.ClientSession()

        try:
            configs_to_load: list[ExchangeConfig] = []
            for name in self._exchange_names:
                config = EXCHANGE_CONFIGS.get(name)
                if not config:
                    logger.warning("unknown_exchange", exchange=name)
                    self._bootstrap_errors[name] = "unknown_exchange"
                    continue
                configs_to_load.append(config)

            pending_configs = list(configs_to_load)
            for attempt in range(1, _BOOTSTRAP_MAX_ATTEMPTS + 1):
                if not pending_configs:
                    break

                # Fetch pending exchanges in parallel. One slow / blocked REST
                # endpoint must not delay bootstrap for the healthy ones.
                tasks = [
                    self._load_exchange(session, config)
                    for config in pending_configs
                ]
                await asyncio.gather(*tasks, return_exceptions=True)

                pending_configs = [
                    config
                    for config in pending_configs
                    if self.get_bootstrap_error(config.name) is not None
                    or not self.get_exchange_symbols(config.name)
                ]

                if pending_configs and attempt < _BOOTSTRAP_MAX_ATTEMPTS:
                    logger.warning(
                        "symbol_bootstrap_retrying",
                        attempt=attempt + 1,
                        max_attempts=_BOOTSTRAP_MAX_ATTEMPTS,
                        exchanges=[config.name for config in pending_configs],
                        delay_seconds=_BOOTSTRAP_RETRY_DELAY_S,
                    )
                    await asyncio.sleep(_BOOTSTRAP_RETRY_DELAY_S)
        finally:
            if own_session:
                await session.close()

        common = self.get_common_symbols()
        failed = {
            name: self.get_bootstrap_error(name) or "0 symbols mapped"
            for name in self._exchange_names
            if not self.get_exchange_symbols(name)
        }
        if failed:
            logger.warning(
                "symbol_mapper_incomplete",
                successes=len(self._exchange_names) - len(failed),
                failures=failed,
            )
        logger.info(
            "symbol_mapper_ready",
            exchanges=self._exchange_names,
            total_common_symbols=len(common),
            successful_exchanges=len(self._exchange_names) - len(failed),
            failed_exchanges=len(failed),
        )

    async def _load_exchange(
        self, session: aiohttp.ClientSession, config: ExchangeConfig
    ) -> None:
        """
        Fetch and parse symbols for one exchange.

        Failures (timeout, geo-block, HTTP error, parse error) are caught so
        one bad exchange never breaks bootstrap for the others. The error
        message is stored in self._bootstrap_errors so /status can surface it.
        """
        native_to_canon: dict[str, str] = {}
        canon_to_native: dict[str, str] = {}
        error_msg: str | None = None
        timeout = aiohttp.ClientTimeout(total=_BOOTSTRAP_TIMEOUT_S)

        try:
            if config.name == "hyperliquid":
                # Hyperliquid uses POST with JSON body for the meta endpoint
                async with session.post(
                    config.rest_url,
                    json={"type": "metaAndAssetCtxs"},
                    headers={"Content-Type": "application/json"},
                    timeout=timeout,
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
            else:
                async with session.get(config.rest_url, timeout=timeout) as resp:
                    resp.raise_for_status()
                    data = await resp.json()

            native_symbols = config.parse_symbols(data)
            logger.info(
                "exchange_symbols_fetched",
                exchange=config.name,
                count=len(native_symbols),
            )

            for native in native_symbols:
                canonical = config.to_canonical(native)
                if canonical is None:
                    logger.debug(
                        "symbol_skipped",
                        exchange=config.name,
                        native=native,
                        reason="no_canonical_match",
                    )
                    continue
                native_to_canon[native] = canonical
                canon_to_native[canonical] = native

        except asyncio.TimeoutError:
            error_msg = f"timeout after {_BOOTSTRAP_TIMEOUT_S}s"
            logger.warning("symbol_fetch_timeout", exchange=config.name, seconds=_BOOTSTRAP_TIMEOUT_S)
        except aiohttp.ClientResponseError as exc:
            # HTTP error: likely 451 (geo-block), 403, 429, etc.
            error_msg = f"HTTP {exc.status} {exc.message}"
            logger.warning(
                "symbol_fetch_http_error",
                exchange=config.name,
                status=exc.status,
                message=exc.message,
            )
        except Exception as exc:
            error_msg = f"{type(exc).__name__}: {exc}"
            logger.exception("symbol_fetch_failed", exchange=config.name)

        self._native_to_canonical[config.name] = native_to_canon
        self._canonical_to_native[config.name] = canon_to_native
        # Record outcome: None == success, string == last error.
        self._bootstrap_errors[config.name] = error_msg

    def load_static(self, exchange: str, mapping: dict[str, str]) -> None:
        """
        Load a static native-to-canonical mapping for testing or manual override.

        Args:
            exchange: Exchange name.
            mapping: Dict of {native_symbol: canonical_symbol}.
        """
        native_to_canon = dict(mapping)
        canon_to_native = {v: k for k, v in mapping.items()}
        self._native_to_canonical[exchange] = native_to_canon
        self._canonical_to_native[exchange] = canon_to_native

    def to_canonical(self, exchange: str, native_symbol: str) -> str | None:
        """
        Convert a native exchange symbol to canonical format.

        Returns None if the symbol is not mapped.
        """
        return self._native_to_canonical.get(exchange, {}).get(native_symbol)

    def to_native(self, exchange: str, canonical_symbol: str) -> str | None:
        """
        Convert a canonical symbol to the native format for an exchange.

        Returns None if the symbol is not available on that exchange.
        """
        return self._canonical_to_native.get(exchange, {}).get(canonical_symbol)

    def get_exchange_symbols(self, exchange: str) -> set[str]:
        """Get all canonical symbols available on one exchange."""
        return set(self._canonical_to_native.get(exchange, {}).keys())

    def get_bootstrap_error(self, exchange: str) -> str | None:
        """
        Return the last bootstrap error for an exchange, or None if it succeeded.
        Returns the literal string 'not_attempted' if bootstrap never ran for it.
        """
        if exchange not in self._bootstrap_errors:
            return "not_attempted"
        return self._bootstrap_errors[exchange]

    def get_common_symbols(self, exchanges: list[str] | None = None) -> set[str]:
        """
        Get canonical symbols available on ALL specified exchanges.

        Args:
            exchanges: Exchanges to intersect. Defaults to all loaded exchanges.

        Returns:
            Set of canonical symbols present on every specified exchange.
        """
        names = exchanges or self._exchange_names
        sets = [self.get_exchange_symbols(name) for name in names if name in self._canonical_to_native]
        if not sets:
            return set()
        return set.intersection(*sets)

    def get_pairwise_common(self) -> dict[tuple[str, str], set[str]]:
        """
        Get common symbols for every pair of exchanges.

        Returns:
            Dict mapping (exchange_a, exchange_b) -> set of common canonical symbols.
        """
        result: dict[tuple[str, str], set[str]] = {}
        names = [n for n in self._exchange_names if n in self._canonical_to_native]
        for i, a in enumerate(names):
            for b in names[i + 1 :]:
                common = self.get_common_symbols([a, b])
                if common:
                    result[(a, b)] = common
        return result

    # ------------------------------------------------------------------
    # Quote-equivalence: USDT ↔ USDC cross-matching
    # ------------------------------------------------------------------

    # Stablecoins considered equivalent for spread detection.
    # They trade near $1 parity so cross-quote spreads are valid.
    EQUIVALENT_QUOTES: list[set[str]] = [{"USDT", "USDC", "BUSD"}]

    @staticmethod
    def extract_base(canonical: str) -> str | None:
        """Extract base asset from canonical symbol. 'APE-USDT-PERP' → 'APE'."""
        parts = canonical.split("-")
        if len(parts) != 3 or parts[2] != "PERP":
            return None
        return parts[0]

    @staticmethod
    def extract_quote(canonical: str) -> str | None:
        """Extract quote asset from canonical symbol. 'APE-USDT-PERP' → 'USDT'."""
        parts = canonical.split("-")
        if len(parts) != 3 or parts[2] != "PERP":
            return None
        return parts[1]

    @classmethod
    def are_quotes_equivalent(cls, quote_a: str, quote_b: str) -> bool:
        """Check if two quote currencies are considered equivalent."""
        if quote_a == quote_b:
            return True
        for group in cls.EQUIVALENT_QUOTES:
            if quote_a in group and quote_b in group:
                return True
        return False

    def get_matchable_pairs(self) -> list[dict]:
        """
        Find all cross-exchange symbol pairs that can be compared for spreads,
        including:
        - Pairs with equivalent but different quote currencies
          (e.g. APE-USDT-PERP on Binance vs APE-USDC-PERP on Hyperliquid)
        - Pairs where the same asset has different tickers across exchanges
          (e.g. 1000PEPE on Binance vs PEPE on Hyperliquid,
           MATIC on one exchange vs POL on another)

        Returns:
            List of dicts:
              {"base": "PEPE",
               "exchange_a": "binance", "canonical_a": "1000PEPE-USDT-PERP",
               "exchange_b": "hyperliquid", "canonical_b": "PEPE-USDC-PERP"}
        """
        # Build: exchange -> {normalized_base: canonical_symbol}
        # Uses ticker aliases so e.g. 1000PEPE and PEPE both normalize to "PEPE"
        # Skips tickers known to collide (same name, different token) across exchanges
        exchange_bases: dict[str, dict[str, str]] = {}
        skipped_collisions: list[str] = []
        for exchange in self._exchange_names:
            bases: dict[str, str] = {}
            for canonical in self.get_exchange_symbols(exchange):
                base = self.extract_base(canonical)
                quote = self.extract_quote(canonical)
                if base and quote:
                    # Normalize base asset name using alias map
                    normalized = normalize_base(base)
                    # Skip known ticker collisions
                    if normalized in TICKER_COLLISIONS:
                        if normalized not in skipped_collisions:
                            skipped_collisions.append(normalized)
                        continue
                    bases[normalized] = canonical
            exchange_bases[exchange] = bases

        if skipped_collisions:
            logger.info(
                "ticker_collisions_skipped",
                count=len(skipped_collisions),
                tickers=skipped_collisions[:10],
            )

        pairs: list[dict] = []
        names = [n for n in self._exchange_names if n in exchange_bases]

        for i, ex_a in enumerate(names):
            for ex_b in names[i + 1:]:
                bases_a = exchange_bases[ex_a]
                bases_b = exchange_bases[ex_b]

                # Find common normalized base assets
                common_bases = set(bases_a.keys()) & set(bases_b.keys())
                for norm_base in common_bases:
                    canon_a = bases_a[norm_base]
                    canon_b = bases_b[norm_base]
                    quote_a = self.extract_quote(canon_a)
                    quote_b = self.extract_quote(canon_b)

                    if quote_a and quote_b and self.are_quotes_equivalent(quote_a, quote_b):
                        pairs.append({
                            "base": norm_base,
                            "exchange_a": ex_a,
                            "canonical_a": canon_a,
                            "exchange_b": ex_b,
                            "canonical_b": canon_b,
                        })

        logger.info("matchable_pairs_found", count=len(pairs))
        return pairs
