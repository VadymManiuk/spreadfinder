"""
Cross-exchange symbol mapper.

Inputs: Native exchange symbols (e.g. "BTCUSDT", "BTC", "BTC_USDT").
Outputs: Canonical symbols ("{BASE}-{QUOTE}-PERP") and reverse lookups.
Assumptions:
  - Only perpetual futures are mapped. Dated futures and spot are excluded.
  - Ambiguous or unrecognized symbols are skipped with a warning log.
  - Symbol lists are bootstrapped from exchange REST APIs on startup.
"""

import structlog
import aiohttp

from symbol_mapper.exchange_symbols import EXCHANGE_CONFIGS, ExchangeConfig

logger = structlog.get_logger(__name__)


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

    async def bootstrap(self, session: aiohttp.ClientSession | None = None) -> None:
        """
        Fetch symbol lists from all configured exchanges and build lookup maps.

        Creates an aiohttp session if none is provided.
        """
        own_session = session is None
        if own_session:
            session = aiohttp.ClientSession()

        try:
            for name in self._exchange_names:
                config = EXCHANGE_CONFIGS.get(name)
                if not config:
                    logger.warning("unknown_exchange", exchange=name)
                    continue
                await self._load_exchange(session, config)
        finally:
            if own_session:
                await session.close()

        common = self.get_common_symbols()
        logger.info(
            "symbol_mapper_ready",
            exchanges=self._exchange_names,
            total_common_symbols=len(common),
        )

    async def _load_exchange(
        self, session: aiohttp.ClientSession, config: ExchangeConfig
    ) -> None:
        """Fetch and parse symbols for one exchange."""
        native_to_canon: dict[str, str] = {}
        canon_to_native: dict[str, str] = {}

        try:
            if config.name == "hyperliquid":
                # Hyperliquid uses POST with JSON body for the meta endpoint
                async with session.post(
                    config.rest_url,
                    json={"type": "metaAndAssetCtxs"},
                    headers={"Content-Type": "application/json"},
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
            else:
                async with session.get(config.rest_url) as resp:
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

        except Exception:
            logger.exception("symbol_fetch_failed", exchange=config.name)

        self._native_to_canonical[config.name] = native_to_canon
        self._canonical_to_native[config.name] = canon_to_native

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
