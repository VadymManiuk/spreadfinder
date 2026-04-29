"""
Exchange-specific symbol format definitions and REST endpoints for fetching
perpetual futures and spot symbol lists.

Inputs: Exchange name.
Outputs: REST URL, response parser, native-to-canonical conversion rules.
Assumptions:
  - Binance perpetuals use USDT-margined contracts with symbol format "BTCUSDT".
  - CEX spot sources use canonical symbols ending in SPOT.
  - Hyperliquid perpetuals use just the base asset name, e.g. "BTC".
  - Gate perpetuals use underscore-separated format, e.g. "BTC_USDT".
"""

from dataclasses import dataclass
from typing import Callable

# Canonical formats:
#   - Futures: {BASE}-{QUOTE}-PERP, e.g. "BTC-USDT-PERP"
#   - Spot:    {BASE}-{QUOTE}-SPOT, e.g. "BTC-USDT-SPOT"

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


def binance_spot_native_to_canonical(native: str) -> str | None:
    """
    Convert Binance spot native symbol to canonical format.

    Binance spot symbols: "BTCUSDT", "AIUSDT", etc.
    Returns None if the symbol doesn't match a stable quote asset.
    """
    for quote in BINANCE_QUOTE_ASSETS:
        if native.endswith(quote):
            base = native[: -len(quote)]
            if base:
                return f"{base}-{quote}-SPOT"
    return None


def binance_spot_canonical_to_native(canonical: str) -> str | None:
    """Convert canonical "BTC-USDT-SPOT" to Binance spot "BTCUSDT"."""
    parts = canonical.split("-")
    if len(parts) != 3 or parts[2] != "SPOT":
        return None
    return f"{parts[0]}{parts[1]}"


def gate_spot_native_to_canonical(native: str) -> str | None:
    """Convert Gate spot "BTC_USDT" to "BTC-USDT-SPOT"."""
    parts = native.split("_")
    if len(parts) != 2:
        return None
    base, quote = parts
    if not base or quote not in BINANCE_QUOTE_ASSETS:
        return None
    return f"{base}-{quote}-SPOT"


def gate_spot_canonical_to_native(canonical: str) -> str | None:
    """Convert canonical "BTC-USDT-SPOT" to Gate spot "BTC_USDT"."""
    parts = canonical.split("-")
    if len(parts) != 3 or parts[2] != "SPOT":
        return None
    return f"{parts[0]}_{parts[1]}"


def bybit_spot_native_to_canonical(native: str) -> str | None:
    """Convert Bybit spot "BTCUSDT" to "BTC-USDT-SPOT"."""
    for quote in BYBIT_QUOTE_ASSETS:
        if native.endswith(quote):
            base = native[: -len(quote)]
            if base:
                return f"{base}-{quote}-SPOT"
    return None


def bybit_spot_canonical_to_native(canonical: str) -> str | None:
    """Convert canonical "BTC-USDT-SPOT" to Bybit spot "BTCUSDT"."""
    parts = canonical.split("-")
    if len(parts) != 3 or parts[2] != "SPOT":
        return None
    return f"{parts[0]}{parts[1]}"


def okx_spot_native_to_canonical(native: str) -> str | None:
    """Convert OKX spot "BTC-USDT" to "BTC-USDT-SPOT"."""
    parts = native.split("-")
    if len(parts) != 2:
        return None
    base, quote = parts
    if not base or quote not in BINANCE_QUOTE_ASSETS:
        return None
    return f"{base}-{quote}-SPOT"


def okx_spot_canonical_to_native(canonical: str) -> str | None:
    """Convert canonical "BTC-USDT-SPOT" to OKX spot "BTC-USDT"."""
    parts = canonical.split("-")
    if len(parts) != 3 or parts[2] != "SPOT":
        return None
    return f"{parts[0]}-{parts[1]}"


def bitget_spot_native_to_canonical(native: str) -> str | None:
    """Convert Bitget spot "BTCUSDT" to "BTC-USDT-SPOT"."""
    for quote in BITGET_QUOTE_ASSETS:
        if native.endswith(quote):
            base = native[: -len(quote)]
            if base:
                return f"{base}-{quote}-SPOT"
    return None


def bitget_spot_canonical_to_native(canonical: str) -> str | None:
    """Convert canonical "BTC-USDT-SPOT" to Bitget spot "BTCUSDT"."""
    parts = canonical.split("-")
    if len(parts) != 3 or parts[2] != "SPOT":
        return None
    return f"{parts[0]}{parts[1]}"


def mexc_spot_native_to_canonical(native: str) -> str | None:
    """Convert MEXC spot "BTCUSDT" to "BTC-USDT-SPOT"."""
    for quote in BINANCE_QUOTE_ASSETS:
        if native.endswith(quote):
            base = native[: -len(quote)]
            if base:
                return f"{base}-{quote}-SPOT"
    return None


def mexc_spot_canonical_to_native(canonical: str) -> str | None:
    """Convert canonical "BTC-USDT-SPOT" to MEXC spot "BTCUSDT"."""
    parts = canonical.split("-")
    if len(parts) != 3 or parts[2] != "SPOT":
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


def _parse_binance_spot_symbols(data: dict) -> list[str]:
    """
    Extract tradable Binance spot symbols from exchangeInfo response.

    Keep only symbols with spot trading enabled and a known quote asset.
    """
    symbols = []
    for s in data.get("symbols", []):
        symbol = s.get("symbol", "")
        if (
            s.get("status") == "TRADING"
            and s.get("isSpotTradingAllowed") is True
            and any(symbol.endswith(quote) for quote in BINANCE_QUOTE_ASSETS)
        ):
            symbols.append(symbol)
    return symbols


def _parse_gate_spot_symbols(data: list) -> list[str]:
    """Extract tradable Gate spot currency pairs."""
    symbols = []
    for pair in data:
        if not isinstance(pair, dict):
            continue
        pair_id = pair.get("id", "")
        quote = pair.get("quote", "")
        trade_status = pair.get("trade_status", "")
        if pair_id and quote in BINANCE_QUOTE_ASSETS and trade_status == "tradable":
            symbols.append(pair_id)
    return symbols


def _parse_bybit_spot_symbols(data: dict) -> list[str]:
    """Extract tradable Bybit spot symbols."""
    result = data.get("result", {})
    symbols = []
    for item in result.get("list", []):
        symbol = item.get("symbol", "")
        if item.get("status") == "Trading" and symbol:
            symbols.append(symbol)
    return symbols


def _parse_okx_spot_symbols(data: dict) -> list[str]:
    """Extract live OKX spot USDT/USDC instruments."""
    symbols = []
    for item in data.get("data", []):
        if (
            item.get("instType") == "SPOT"
            and item.get("state") == "live"
            and item.get("quoteCcy") in BINANCE_QUOTE_ASSETS
        ):
            symbols.append(item["instId"])
    return symbols


def _parse_bitget_spot_symbols(data: dict) -> list[str]:
    """Extract online Bitget spot symbols."""
    symbols = []
    for item in data.get("data", []):
        symbol = item.get("symbol", "")
        quote = item.get("quoteCoin", "")
        status = item.get("status", "")
        if symbol and quote in BINANCE_QUOTE_ASSETS and status == "online":
            symbols.append(symbol)
    return symbols


def _parse_mexc_spot_symbols(data: dict) -> list[str]:
    """Extract enabled MEXC spot symbols."""
    symbols = []
    for item in data.get("symbols", []):
        symbol = item.get("symbol", "")
        quote = item.get("quoteAsset", "")
        status = str(item.get("status", "")).upper()
        if symbol and quote in BINANCE_QUOTE_ASSETS and status in {"1", "ENABLED", "TRADING"}:
            symbols.append(symbol)
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


# Bybit linear perps: same format as Binance ("BTCUSDT")
BYBIT_QUOTE_ASSETS = ("USDT", "USDC")

# OKX perp swaps: "BTC-USDT-SWAP"
OKX_QUOTE = "USDT"

# Bitget USDT-futures: same format as Binance ("BTCUSDT")
BITGET_QUOTE_ASSETS = ("USDT",)


def bybit_native_to_canonical(native: str) -> str | None:
    """Convert Bybit "BTCUSDT" to "BTC-USDT-PERP"."""
    for quote in BYBIT_QUOTE_ASSETS:
        if native.endswith(quote):
            base = native[: -len(quote)]
            if base:
                return f"{base}-{quote}-PERP"
    return None


def bybit_canonical_to_native(canonical: str) -> str | None:
    """Convert "BTC-USDT-PERP" to Bybit "BTCUSDT"."""
    parts = canonical.split("-")
    if len(parts) != 3 or parts[2] != "PERP":
        return None
    return f"{parts[0]}{parts[1]}"


def okx_native_to_canonical(native: str) -> str | None:
    """Convert OKX "BTC-USDT-SWAP" to "BTC-USDT-PERP"."""
    parts = native.split("-")
    if len(parts) != 3 or parts[2] != "SWAP":
        return None
    return f"{parts[0]}-{parts[1]}-PERP"


def okx_canonical_to_native(canonical: str) -> str | None:
    """Convert "BTC-USDT-PERP" to OKX "BTC-USDT-SWAP"."""
    parts = canonical.split("-")
    if len(parts) != 3 or parts[2] != "PERP":
        return None
    return f"{parts[0]}-{parts[1]}-SWAP"


def bitget_native_to_canonical(native: str) -> str | None:
    """Convert Bitget "BTCUSDT" to "BTC-USDT-PERP"."""
    for quote in BITGET_QUOTE_ASSETS:
        if native.endswith(quote):
            base = native[: -len(quote)]
            if base:
                return f"{base}-{quote}-PERP"
    return None


def bitget_canonical_to_native(canonical: str) -> str | None:
    """Convert "BTC-USDT-PERP" to Bitget "BTCUSDT"."""
    parts = canonical.split("-")
    if len(parts) != 3 or parts[2] != "PERP":
        return None
    return f"{parts[0]}{parts[1]}"


def _parse_bybit_symbols(data: dict) -> list[str]:
    """Extract linear perpetual symbols from Bybit instruments-info response."""
    symbols = []
    result = data.get("result", {})
    for item in result.get("list", []):
        if (
            item.get("contractType") == "LinearPerpetual"
            and item.get("status") == "Trading"
        ):
            symbols.append(item["symbol"])
    return symbols


def _parse_okx_symbols(data: dict) -> list[str]:
    """Extract USDT-margined perpetual swap instruments from OKX response."""
    symbols = []
    for item in data.get("data", []):
        if (
            item.get("instType") == "SWAP"
            and item.get("ctType") == "linear"
            and item.get("state") == "live"
            and item.get("settleCcy") == "USDT"
        ):
            symbols.append(item["instId"])
    return symbols


def _parse_bitget_symbols(data: dict) -> list[str]:
    """Extract USDT-futures symbols from Bitget contracts response."""
    symbols = []
    for item in data.get("data", []):
        if item.get("symbolStatus") == "normal":
            symbols.append(item["symbol"])
    return symbols


# Aster: Binance API clone — same format ("BTCUSDT")
ASTER_QUOTE_ASSETS = ("USDT",)


def aster_native_to_canonical(native: str) -> str | None:
    """Convert Aster "BTCUSDT" to "BTC-USDT-PERP"."""
    for quote in ASTER_QUOTE_ASSETS:
        if native.endswith(quote):
            base = native[: -len(quote)]
            if base:
                return f"{base}-{quote}-PERP"
    return None


def aster_canonical_to_native(canonical: str) -> str | None:
    """Convert "BTC-USDT-PERP" to Aster "BTCUSDT"."""
    parts = canonical.split("-")
    if len(parts) != 3 or parts[2] != "PERP":
        return None
    return f"{parts[0]}{parts[1]}"


def _parse_aster_symbols(data: dict) -> list[str]:
    """Extract perpetual symbols from Aster exchangeInfo (Binance clone)."""
    symbols = []
    for s in data.get("symbols", []):
        if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING":
            symbols.append(s["symbol"])
    return symbols


# Lighter: symbols are base asset only ("ETH", "BTC"), quoted in USDC.
# Markets identified by index. Non-crypto perps (stocks, forex, commodities) are excluded.
LIGHTER_QUOTE = "USDC"

# Symbols that are NOT crypto and should be excluded from matching
LIGHTER_NON_CRYPTO = {
    "SPX", "SPY", "QQQ", "IWM", "DIA", "BOTZ", "MAGS", "URA", "ROBO",  # ETFs/indices
    "HOOD", "COIN", "NVDA", "PLTR", "TSLA", "AAPL", "AMZN", "MSFT",     # stocks
    "GOOGL", "META", "INTC", "AMD", "SNDK", "SAMSUNG", "HYUNDAI",        # stocks
    "KRCOMP", "SKHYNIX", "ASML", "MSTR", "HANMI",                        # stocks
    "XAU", "XAG", "XCU", "XPD", "XPT",                                   # metals
    "WTI", "BRENTOIL", "NATGAS",                                          # commodities
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD",                   # forex
    "AUDUSD", "NZDUSD", "USDKRW",                                        # forex
}


def lighter_native_to_canonical(native: str) -> str | None:
    """Convert Lighter "ETH" to "ETH-USDC-PERP"."""
    if not native or "-" in native or "/" in native:
        return None
    if native in LIGHTER_NON_CRYPTO:
        return None
    return f"{native}-{LIGHTER_QUOTE}-PERP"


def lighter_canonical_to_native(canonical: str) -> str | None:
    """Convert "ETH-USDC-PERP" to Lighter "ETH"."""
    parts = canonical.split("-")
    if len(parts) != 3 or parts[2] != "PERP":
        return None
    if parts[1] != LIGHTER_QUOTE:
        return None
    return parts[0]


def _parse_lighter_symbols(data: list) -> list[str]:
    """
    Extract perp symbols from Lighter markets response.

    Response format: [{"symbol": "ETH", "market_index": 0}, ...]
    Only include perp markets (index < 2048, spot >= 2048).
    Excludes non-crypto symbols (stocks, forex, commodities).
    """
    symbols = []
    for item in data:
        if isinstance(item, dict):
            idx = item.get("market_index", 9999)
            sym = item.get("symbol", "")
            # Perps have index < 2048, spots >= 2048
            if idx < 2048 and sym and sym not in LIGHTER_NON_CRYPTO:
                # Skip symbols with "/" (spot pairs like "ETH/USDC")
                if "/" not in sym:
                    symbols.append(sym)
    return symbols


# Lighter also needs market_index for WS subscriptions.
# This is stored separately and fetched during bootstrap.
LIGHTER_MARKET_INDEX_MAP: dict[str, int] = {}


def lighter_parse_and_store_indices(data: list) -> list[str]:
    """Parse symbols AND store market indices for WS subscription."""
    LIGHTER_MARKET_INDEX_MAP.clear()
    symbols = []
    for item in data:
        if isinstance(item, dict):
            idx = item.get("market_index", 9999)
            sym = item.get("symbol", "")
            if idx < 2048 and sym and sym not in LIGHTER_NON_CRYPTO and "/" not in sym:
                symbols.append(sym)
                LIGHTER_MARKET_INDEX_MAP[sym] = idx
    return symbols


# MEXC futures: same underscore format as Gate ("BTC_USDT")
MEXC_QUOTE = "USDT"


def mexc_native_to_canonical(native: str) -> str | None:
    """Convert MEXC "BTC_USDT" to "BTC-USDT-PERP"."""
    parts = native.split("_")
    if len(parts) != 2:
        return None
    base, quote = parts
    if not base or not quote:
        return None
    return f"{base}-{quote}-PERP"


def mexc_canonical_to_native(canonical: str) -> str | None:
    """Convert "BTC-USDT-PERP" to MEXC "BTC_USDT"."""
    parts = canonical.split("-")
    if len(parts) != 3 or parts[2] != "PERP":
        return None
    return f"{parts[0]}_{parts[1]}"


def _parse_mexc_symbols(data: dict) -> list[str]:
    """
    Extract perpetual futures symbols from MEXC contract/detail response.

    Response: {"success": true, "data": [{"symbol": "BTC_USDT", "state": 0, ...}, ...]}
    state=0 means active.
    """
    symbols = []
    for item in data.get("data", []):
        if isinstance(item, dict) and item.get("state") == 0:
            sym = item.get("symbol", "")
            if sym:
                symbols.append(sym)
    return symbols


# Registry of supported exchanges
EXCHANGE_CONFIGS: dict[str, ExchangeConfig] = {
    "binance_spot": ExchangeConfig(
        name="binance_spot",
        rest_url="https://api.binance.com/api/v3/exchangeInfo",
        to_canonical=binance_spot_native_to_canonical,
        to_native=binance_spot_canonical_to_native,
        parse_symbols=_parse_binance_spot_symbols,
    ),
    "gate_spot": ExchangeConfig(
        name="gate_spot",
        rest_url="https://api.gateio.ws/api/v4/spot/currency_pairs",
        to_canonical=gate_spot_native_to_canonical,
        to_native=gate_spot_canonical_to_native,
        parse_symbols=_parse_gate_spot_symbols,
    ),
    "bybit_spot": ExchangeConfig(
        name="bybit_spot",
        rest_url="https://api.bybit.com/v5/market/instruments-info?category=spot",
        to_canonical=bybit_spot_native_to_canonical,
        to_native=bybit_spot_canonical_to_native,
        parse_symbols=_parse_bybit_spot_symbols,
    ),
    "okx_spot": ExchangeConfig(
        name="okx_spot",
        rest_url="https://www.okx.com/api/v5/public/instruments?instType=SPOT",
        to_canonical=okx_spot_native_to_canonical,
        to_native=okx_spot_canonical_to_native,
        parse_symbols=_parse_okx_spot_symbols,
    ),
    "bitget_spot": ExchangeConfig(
        name="bitget_spot",
        rest_url="https://api.bitget.com/api/v2/spot/public/symbols",
        to_canonical=bitget_spot_native_to_canonical,
        to_native=bitget_spot_canonical_to_native,
        parse_symbols=_parse_bitget_spot_symbols,
    ),
    "mexc_spot": ExchangeConfig(
        name="mexc_spot",
        rest_url="https://api.mexc.com/api/v3/exchangeInfo",
        to_canonical=mexc_spot_native_to_canonical,
        to_native=mexc_spot_canonical_to_native,
        parse_symbols=_parse_mexc_spot_symbols,
    ),
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
    "bybit": ExchangeConfig(
        name="bybit",
        rest_url="https://api.bybit.com/v5/market/instruments-info?category=linear",
        to_canonical=bybit_native_to_canonical,
        to_native=bybit_canonical_to_native,
        parse_symbols=_parse_bybit_symbols,
    ),
    "okx": ExchangeConfig(
        name="okx",
        rest_url="https://www.okx.com/api/v5/public/instruments?instType=SWAP",
        to_canonical=okx_native_to_canonical,
        to_native=okx_canonical_to_native,
        parse_symbols=_parse_okx_symbols,
    ),
    "bitget": ExchangeConfig(
        name="bitget",
        rest_url="https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES",
        to_canonical=bitget_native_to_canonical,
        to_native=bitget_canonical_to_native,
        parse_symbols=_parse_bitget_symbols,
    ),
    "aster": ExchangeConfig(
        name="aster",
        rest_url="https://fapi.asterdex.com/fapi/v3/exchangeInfo",
        to_canonical=aster_native_to_canonical,
        to_native=aster_canonical_to_native,
        parse_symbols=_parse_aster_symbols,
    ),
    "lighter": ExchangeConfig(
        name="lighter",
        rest_url="https://explorer.elliot.ai/api/markets",
        to_canonical=lighter_native_to_canonical,
        to_native=lighter_canonical_to_native,
        parse_symbols=lighter_parse_and_store_indices,
    ),
    "mexc": ExchangeConfig(
        name="mexc",
        rest_url="https://contract.mexc.com/api/v1/contract/detail",
        to_canonical=mexc_native_to_canonical,
        to_native=mexc_canonical_to_native,
        parse_symbols=_parse_mexc_symbols,
    ),
}
