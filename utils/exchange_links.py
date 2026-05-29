"""
Exchange trading-pair URL builder.

Inputs: Exchange identifier (e.g. "binance"), base token ticker (e.g. "ARIA").
Outputs: HTTPS URL to the exchange's trading page for that
         pair, or None if the exchange is unknown.
Assumptions:
  - All supported exchanges list USDT-margined perps — so the quote is USDT.
  - Perp markets are preferred except for explicitly named spot venues.
  - Base tickers arrive in uppercase canonical form (e.g. "BTC", "SPACEX").
"""

from typing import Callable

from utils.venues import exchange_family

# One template (or builder function) per supported exchange.
# Keys match the `exchange` field on MarketSnapshot / SpreadOpportunity.
#
# All URLs point at the USDT-perpetual futures market for `{BASE}`.
#
# Verified by hand against each exchange's public trading UI as of 2026-04.
# If an exchange redesigns their URL scheme, update the entry here — callers
# don't need to change.
_BUILDERS: dict[str, Callable[[str], str]] = {
    # CEXes
    "binance_spot": lambda b: f"https://www.binance.com/en/trade/{b}_USDT?type=spot",
    "gate_spot":    lambda b: f"https://www.gate.io/trade/{b}_USDT",
    "bybit_spot":   lambda b: f"https://www.bybit.com/en/trade/spot/{b}/USDT",
    "okx_spot":     lambda b: f"https://www.okx.com/trade-spot/{b.lower()}-usdt",
    "bitget_spot":  lambda b: f"https://www.bitget.com/spot/{b}USDT",
    "mexc_spot":    lambda b: f"https://www.mexc.com/exchange/{b}_USDT",
    "binance": lambda b: f"https://www.binance.com/en/futures/{b}USDT",
    "bybit":   lambda b: f"https://www.bybit.com/trade/usdt/{b}USDT",
    "gate":    lambda b: f"https://www.gate.io/futures/USDT/{b}_USDT",
    "okx":     lambda b: f"https://www.okx.com/trade-swap/{b.lower()}-usdt-swap",
    "bitget":  lambda b: f"https://www.bitget.com/futures/usdt/{b}USDT",
    "mexc":    lambda b: f"https://futures.mexc.com/exchange/{b}_USDT",
    # DEX perps
    "hyperliquid": lambda b: f"https://app.hyperliquid.xyz/trade/{b}",
    "aster":       lambda b: f"https://www.asterdex.com/en/futures/v1/{b}USDT",
    "lighter":     lambda b: f"https://app.lighter.xyz/trade/{b}",
    # On-chain DEX sources. These feeds do not currently carry token contract
    # addresses, so the URL opens the swap/search surface instead of a pair page.
    "binance_alpha": lambda b: "https://www.binance.com/en/alpha",
    "okx_dex":       lambda b: "https://www.okx.com/web3/dex-swap",
}


def futures_url(exchange: str, base: str) -> str | None:
    """
    Build the perpetual-futures trading URL for `base` on `exchange`.

    Returns None if the exchange has no known URL template — callers should
    degrade gracefully (skip the link, don't crash).
    """
    if not exchange or not base:
        return None
    builder = _BUILDERS.get(exchange_family(exchange.lower()))
    if builder is None:
        return None
    return builder(base.upper())


def supported_exchanges() -> list[str]:
    """All exchanges we know how to build futures URLs for."""
    return sorted(_BUILDERS.keys())
