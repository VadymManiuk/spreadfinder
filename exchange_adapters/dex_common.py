"""
Shared helpers for DEX market polling adapters.

Inputs: Price and liquidity values returned by DEX APIs.
Outputs: Conservative executable-size estimates used by downstream filters.
Assumptions:
  - Pool liquidity is quoted in USD and spans both sides of the market.
  - One-side size is approximated as half the liquidity divided by price.
"""

from decimal import Decimal


def estimate_size_from_liquidity(
    price: Decimal,
    liquidity_usd: Decimal | None,
) -> Decimal:
    """
    Approximate one-side executable size from pooled liquidity.

    size ~= liquidity_usd / price / 2
    The /2 keeps the estimate conservative because pool liquidity spans both sides.
    """
    if liquidity_usd is None or liquidity_usd <= 0 or price <= 0:
        return Decimal("0")
    return liquidity_usd / price / Decimal("2")
