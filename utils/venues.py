"""
Helpers for classifying venue families and rendering human-friendly names.

Inputs: Raw exchange/source identifiers stored on MarketSnapshot objects.
Outputs: Booleans for DEX-vs-futures routing and display labels for alerts.
Assumptions:
  - DEX sources are encoded as "<family>:<chain_id>" (e.g. "okx_dex:8453").
  - Spot venues use a dedicated family such as "binance_spot".
  - Centralized futures venues keep their plain exchange name
    (e.g. "gate", "binance", "hyperliquid").
"""

DEX_EXCHANGE_FAMILIES = {
    "okx_dex",
    "binance_alpha",
}

SPOT_EXCHANGE_FAMILIES = {
    "binance_spot",
}

_DISPLAY_NAMES = {
    "okx_dex": "OKX DEX",
    "binance_alpha": "Binance Alpha",
    "binance_spot": "Binance Spot",
    "binance": "Binance",
    "hyperliquid": "Hyperliquid",
    "gate": "Gate",
    "bybit": "Bybit",
    "okx": "OKX",
    "bitget": "Bitget",
    "aster": "Aster",
    "lighter": "Lighter",
    "mexc": "MEXC",
}

_CHAIN_LABELS = {
    "1": "Ethereum",
    "56": "BSC",
    "42161": "Arbitrum",
    "8453": "Base",
    "501": "Solana",
    "CT_501": "Solana",
}


def exchange_family(exchange: str) -> str:
    """Return the venue family, dropping any chain suffix."""
    if not exchange:
        return ""
    return exchange.split(":", 1)[0]


def exchange_chain(exchange: str) -> str | None:
    """Return the optional chain suffix encoded on a DEX venue string."""
    if ":" not in exchange:
        return None
    return exchange.split(":", 1)[1]


def is_dex_exchange(exchange: str) -> bool:
    """True when the exchange/source represents an on-chain DEX feed."""
    return exchange_family(exchange) in DEX_EXCHANGE_FAMILIES


def is_spot_exchange(exchange: str) -> bool:
    """True when the exchange/source represents a centralized spot feed."""
    return exchange_family(exchange) in SPOT_EXCHANGE_FAMILIES


def display_exchange(exchange: str) -> str:
    """Convert an internal venue id into a human-friendly label."""
    family = exchange_family(exchange)
    label = _DISPLAY_NAMES.get(family, exchange)
    chain = exchange_chain(exchange)
    if chain and is_dex_exchange(exchange):
        chain_label = _CHAIN_LABELS.get(chain, chain)
        return f"{label} ({chain_label})"
    return label
