"""
Exchange-specific symbol format definitions and REST endpoints for fetching
perpetual futures symbol lists.

Inputs: Exchange name.
Outputs: REST URL, response parser, native-to-canonical conversion rules.
Assumptions:
  - Binance perpetuals use USDT-margined contracts with symbol format "BTCUSDT".
  - Hyperliquid perpetuals use just the base asset name, e.g. "BTC".
  - Gate perpetuals use underscore-separated format, e.g. "BTC_USDT".
"""

from dataclasses import dataclass
from typing import Callable

# Canonical format: {BASE}-{QUOTE}-PERP
# Example: "BTC-USDT-PERP"

# Known USDT-quoted stablecoins and quote assets to strip from Binance symbols.
# TODO — review this list periodically; new quote assets may appear.
BINANCE_QUOTE_ASSETS = ("USDT", "USDC", "BUSD")

# Hyperliquid quotes everything in USDC internally but symbols are just base names.
HYPERLIQUID_QUOTE = "USDC"

# Gate futures default quote
GATE_QUOTE = "USDT"


def binance_native_to_canonical(native: str) -> str | None:
    """
    Convert Binance native symbol to canonical format.

    Binance perpetual symbols: "BTCUSDT", "ETHUSDT", etc.
    Returns None if the symbol doesn't match a known quote asset.
    """
    for quote in BINANCE_QUOTE_ASSETS:
        if native.endswith(quote):
            base = native[: -len(quote)]
            if base:  # guard against empty base
                return f"{base}-{quote}-PERP"
    return None


def binance_canonical_to_native(canonical: str) -> str | None:
    """Convert canonical "BTC-USDT-PERP" to Binance "BTCUSDT"."""
    parts = canonical.split("-")
    if len(parts) != 3 or parts[2] != "PERP":
        return None
    return f"{parts[0]}{parts[1]}"


def hyperliquid_native_to_canonical(native: str) -> str | None:
    """
    Convert Hyperliquid native symbol to canonical format.

    Hyperliquid symbols are just the base asset: "BTC", "ETH", etc.
    All are quoted in USDC.
    """
    if not native or "-" in native:
        return None
    return f"{native}-{HYPERLIQUID_QUOTE}-PERP"


def hyperliquid_canonical_to_native(canonical: str) -> str | None:
    """Convert canonical "BTC-USDC-PERP" to Hyperliquid "BTC"."""
    parts = canonical.split("-")
    if len(parts) != 3 or parts[2] != "PERP":
        return None
    if parts[1] != HYPERLIQUID_QUOTE:
        return None
    return parts[0]


def gate_native_to_canonical(native: str) -> str | None:
    """
    Convert Gate native symbol to canonical format.

    Gate perpetual symbols: "BTC_USDT", "ETH_USDT", etc.
    """
    parts = native.split("_")
    if len(parts) != 2:
        return None
    base, quote = parts
    if not base or not quote:
        return None
    return f"{base}-{quote}-PERP"


def gate_canonical_to_native(canonical: str) -> str | None:
    """Convert canonical "BTC-USDT-PERP" to Gate "BTC_USDT"."""
    parts = canonical.split("-")
    if len(parts) != 3 or parts[2] != "PERP":
        return None
    return f"{parts[0]}_{parts[1]}"


@dataclass(frozen=True)
class ExchangeConfig:
    """Configuration for one exchange's symbol handling."""

    name: str
    rest_url: str
    to_canonical: Callable[[str], str | None]
    to_native: Callable[[str], str | None]
    parse_symbols: Callable[[dict | list], list[str]]


def _parse_binance_symbols(data: dict) -> list[str]:
    """
    Extract perpetual futures symbols from Binance exchangeInfo response.

    Filters for PERPETUAL contract type and TRADING status only.
    """
    symbols = []
    for s in data.get("symbols", []):
        if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING":
            symbols.append(s["symbol"])
    return symbols


def _parse_hyperliquid_symbols(data: list) -> list[str]:
    """
    Extract perpetual symbols from Hyperliquid meta response.

    The meta endpoint returns [{"universe": [{"name": "BTC", ...}, ...]}, ...].
    """
    # TODO — verify exact response structure against live API
    if isinstance(data, list) and len(data) > 0:
        meta = data[0] if isinstance(data[0], dict) else data
        if isinstance(meta, dict):
            universe = meta.get("universe", [])
            return [asset["name"] for asset in universe if "name" in asset]
    return []


def _parse_gate_symbols(data: list) -> list[str]:
    """
    Extract perpetual futures symbols from Gate contracts response.

    Filters for active contracts (not in settlement/delisted state).
    """
    symbols = []
    for contract in data:
        if isinstance(contract, dict) and contract.get("in_delisting") is not True:
            name = contract.get("name", "")
            if name:
                symbols.append(name)
    return symbols


# Registry of supported exchanges
EXCHANGE_CONFIGS: dict[str, ExchangeConfig] = {
    "binance": ExchangeConfig(
        name="binance",
        rest_url="https://fapi.binance.com/fapi/v1/exchangeInfo",
        to_canonical=binance_native_to_canonical,
        to_native=binance_canonical_to_native,
        parse_symbols=_parse_binance_symbols,
    ),
    "hyperliquid": ExchangeConfig(
        name="hyperliquid",
        rest_url="https://api.hyperliquid.xyz/info",
        to_canonical=hyperliquid_native_to_canonical,
        to_native=hyperliquid_canonical_to_native,
        parse_symbols=_parse_hyperliquid_symbols,
    ),
    "gate": ExchangeConfig(
        name="gate",
        rest_url="https://api.gateio.ws/api/v4/futures/usdt/contracts",
        to_canonical=gate_native_to_canonical,
        to_native=gate_canonical_to_native,
        parse_symbols=_parse_gate_symbols,
    ),
}
