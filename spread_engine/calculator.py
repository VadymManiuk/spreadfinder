"""
Cross-exchange spread calculator.

Inputs: Two MarketSnapshot objects for the same canonical symbol on different exchanges.
Outputs: Up to two SpreadOpportunity objects (one per direction).
Assumptions:
  - Fee estimates are rough defaults per exchange. Marked as ESTIMATE.
  - Slippage factor is a flat multiplier on mid price. Marked as ESTIMATE.
  - Both directions (A→B and B→A) are checked.
"""

from datetime import datetime, timezone
from decimal import Decimal

import structlog

from models.snapshot import MarketSnapshot, SpreadOpportunity
from spread_engine.confidence import calculate_confidence
from symbol_mapper.ticker_aliases import normalize_base
from utils.venues import exchange_family

logger = structlog.get_logger(__name__)

# Tokens with 1000x multiplier on some exchanges.
# If one exchange lists "1000PEPE" and another lists "PEPE",
# the 1000x exchange price is 1000 * the raw token price.
# We must divide the 1000x price by 1000 before comparing.
_1000X_PREFIX = "1000"

# Default fee rates per exchange (maker + taker for a round trip)
# ESTIMATE — these are standard tiers, actual rates depend on VIP level
DEFAULT_FEES: dict[str, tuple[Decimal, Decimal]] = {
    # (maker_rate, taker_rate)
    "binance":     (Decimal("0.0002"), Decimal("0.0004")),   # 0.02% maker, 0.04% taker
    "hyperliquid": (Decimal("0.0002"), Decimal("0.0005")),   # 0.02% maker, 0.05% taker
    "gate":        (Decimal("0.00015"), Decimal("0.0005")),  # 0.015% maker, 0.05% taker
    "bybit":       (Decimal("0.0002"), Decimal("0.00055")),  # 0.02% maker, 0.055% taker  # ESTIMATE
    "okx":         (Decimal("0.0002"), Decimal("0.0005")),   # 0.02% maker, 0.05% taker   # ESTIMATE
    "bitget":      (Decimal("0.0002"), Decimal("0.0006")),   # 0.02% maker, 0.06% taker   # ESTIMATE
    "aster":       (Decimal("0.0002"), Decimal("0.0005")),   # 0.02% maker, 0.05% taker   # ESTIMATE
    "lighter":     (Decimal("0.0002"), Decimal("0.0005")),   # 0.02% maker, 0.05% taker   # ESTIMATE
    "mexc":        (Decimal("0.0002"), Decimal("0.0006")),   # 0.02% maker, 0.06% taker   # ESTIMATE
    "okx_dex":     (Decimal("0.0030"), Decimal("0.0030")),   # ~0.30% swap cost            # ESTIMATE
    "binance_alpha": (Decimal("0.0030"), Decimal("0.0030")), # ~0.30% swap cost            # ESTIMATE
}

# Slippage factor as fraction of mid price
# ESTIMATE — small caps will have higher slippage than this
SLIPPAGE_FACTOR = Decimal("0.0001")  # 1 bps


def _get_fee_rate(exchange: str, side: str) -> Decimal:
    """
    Get fee rate for an exchange.

    Args:
        exchange: Exchange name.
        side: "maker" or "taker".

    Returns:
        Fee rate as a decimal fraction (e.g. 0.0004 for 0.04%).
    """
    rates = DEFAULT_FEES.get(
        exchange_family(exchange),
        (Decimal("0.0005"), Decimal("0.0005")),
    )
    return rates[0] if side == "maker" else rates[1]


def _estimate_fees(buy_exchange: str, sell_exchange: str, mid_price: Decimal) -> Decimal:
    """
    Estimate round-trip fees for a spread trade.

    estimated_fees = (maker_fee_sell + taker_fee_buy) * mid_price * 2

    Assumption: you'd take (taker) on the buy side, make (maker) on the sell side.
    ESTIMATE — refine later with actual fee tiers.
    """
    taker_buy = _get_fee_rate(buy_exchange, "taker")
    maker_sell = _get_fee_rate(sell_exchange, "maker")

    # estimated_fees = (taker_fee_buy + maker_fee_sell) * mid_price * 2
    return (taker_buy + maker_sell) * mid_price * 2


def _estimate_slippage(mid_price: Decimal) -> Decimal:
    """
    Estimate slippage cost.

    estimated_slippage = slippage_factor * mid_price
    ESTIMATE — small-cap tokens will have significantly more slippage.
    """
    # estimated_slippage = SLIPPAGE_FACTOR * mid_price
    return SLIPPAGE_FACTOR * mid_price


def _extract_base(canonical: str) -> str | None:
    """Extract base asset from canonical symbol. 'APE-USDT-PERP' → 'APE'."""
    parts = canonical.split("-")
    return parts[0] if len(parts) == 3 else None


def _get_price_multiplier(base: str) -> Decimal:
    """
    Get the price multiplier for a base asset.

    Tokens listed as "1000PEPE" have prices that are 1000x the raw token price.
    When comparing with an exchange that lists plain "PEPE", we need to
    divide the 1000x price by 1000.

    Returns 1000 if the base has a 1000x prefix, 1 otherwise.
    """
    if base.startswith(_1000X_PREFIX):
        return Decimal("1000")
    if base.startswith("k"):
        # Hyperliquid uses "k" prefix for 1000x tokens (e.g., kPEPE)
        return Decimal("1000")
    return Decimal("1")


def _normalize_snapshot_prices(
    snap: MarketSnapshot, multiplier: Decimal
) -> MarketSnapshot:
    """
    Return a price-adjusted copy of the snapshot if the token has a multiplier.

    For 1000x tokens: divide prices by 1000, multiply sizes by 1000
    (since 1 unit of 1000PEPE = 1000 units of PEPE).
    """
    if multiplier == 1:
        return snap

    return MarketSnapshot(
        canonical_symbol=snap.canonical_symbol,
        exchange=snap.exchange,
        bid=snap.bid / multiplier,
        ask=snap.ask / multiplier,
        bid_size=snap.bid_size * multiplier,
        ask_size=snap.ask_size * multiplier,
        exchange_ts=snap.exchange_ts,
        local_ts=snap.local_ts,
        mark_price=snap.mark_price / multiplier if snap.mark_price else None,
        index_price=snap.index_price / multiplier if snap.index_price else None,
        funding_rate=snap.funding_rate,
        volume_24h=snap.volume_24h,
        is_stale=snap.is_stale,
    )


def calculate_spread(
    snap_a: MarketSnapshot,
    snap_b: MarketSnapshot,
    now: datetime | None = None,
) -> list[SpreadOpportunity]:
    """
    Calculate spread opportunities between two snapshots.

    Supports:
      - Same-symbol comparisons (APE-USDT-PERP vs APE-USDT-PERP)
      - Cross-quote comparisons (APE-USDT-PERP vs APE-USDC-PERP)
      - Cross-ticker comparisons (1000PEPE-USDT-PERP vs PEPE-USDC-PERP)
        with automatic 1000x price normalization

    Checks both directions:
      Direction 1: Buy on A (at A.ask), sell on B (at B.bid)
      Direction 2: Buy on B (at B.ask), sell on A (at A.bid)

    Returns a list of 0–2 SpreadOpportunity objects (only positive gross spreads).
    """
    base_a = _extract_base(snap_a.canonical_symbol)
    base_b = _extract_base(snap_b.canonical_symbol)

    if base_a is None or base_b is None:
        return []

    # Normalize base names using alias map
    norm_a = normalize_base(base_a)
    norm_b = normalize_base(base_b)

    if norm_a != norm_b:
        logger.warning(
            "symbol_mismatch",
            symbol_a=snap_a.canonical_symbol,
            symbol_b=snap_b.canonical_symbol,
            normalized_a=norm_a,
            normalized_b=norm_b,
        )
        return []

    # Handle 1000x price normalization
    mult_a = _get_price_multiplier(base_a)
    mult_b = _get_price_multiplier(base_b)
    adj_a = _normalize_snapshot_prices(snap_a, mult_a)
    adj_b = _normalize_snapshot_prices(snap_b, mult_b)

    ref = now or datetime.now(timezone.utc)
    opportunities: list[SpreadOpportunity] = []

    # Direction 1: buy on A, sell on B
    opp = _calc_one_direction(adj_a, adj_b, ref)
    if opp:
        opportunities.append(opp)

    # Direction 2: buy on B, sell on A
    opp = _calc_one_direction(adj_b, adj_a, ref)
    if opp:
        opportunities.append(opp)

    return opportunities


def _calc_one_direction(
    buy_snap: MarketSnapshot,
    sell_snap: MarketSnapshot,
    now: datetime,
) -> SpreadOpportunity | None:
    """
    Calculate spread for one direction: buy on buy_snap's exchange, sell on sell_snap's exchange.

    gross_spread = sell_snap.bid - buy_snap.ask
    gross_spread_bps = (gross_spread / buy_snap.ask) * 10000
    net_spread = gross_spread - estimated_fees - estimated_slippage
    net_spread_bps = (net_spread / buy_snap.ask) * 10000
    """
    buy_ask = buy_snap.ask
    sell_bid = sell_snap.bid

    # gross_spread = sell_bid - buy_ask
    gross_spread = sell_bid - buy_ask

    if gross_spread <= 0:
        return None

    if buy_ask == 0:
        return None

    # gross_spread_bps = (gross_spread / buy_ask) * 10000
    gross_spread_bps = (gross_spread / buy_ask) * 10000

    # Cost estimates
    mid_price = (buy_ask + sell_bid) / 2
    estimated_fees = _estimate_fees(buy_snap.exchange, sell_snap.exchange, mid_price)
    estimated_slippage = _estimate_slippage(mid_price)

    # net_spread = gross_spread - estimated_fees - estimated_slippage
    net_spread = gross_spread - estimated_fees - estimated_slippage

    # net_spread_bps = (net_spread / buy_ask) * 10000
    net_spread_bps = (net_spread / buy_ask) * 10000

    data_age = max(buy_snap.data_age_ms(now), sell_snap.data_age_ms(now))

    confidence = calculate_confidence(
        snap_a=buy_snap,
        snap_b=sell_snap,
        buy_ask_size=buy_snap.ask_size,
        sell_bid_size=sell_snap.bid_size,
        gross_spread_bps=gross_spread_bps,
        now=now,
    )

    return SpreadOpportunity(
        canonical_symbol=buy_snap.canonical_symbol,
        buy_exchange=buy_snap.exchange,
        sell_exchange=sell_snap.exchange,
        buy_ask=buy_ask,
        sell_bid=sell_bid,
        gross_spread=gross_spread,
        gross_spread_bps=gross_spread_bps,
        net_spread=net_spread,
        net_spread_bps=net_spread_bps,
        estimated_fees=estimated_fees,
        estimated_slippage=estimated_slippage,
        buy_funding_rate=buy_snap.funding_rate,
        sell_funding_rate=sell_snap.funding_rate,
        buy_ask_size=buy_snap.ask_size,
        sell_bid_size=sell_snap.bid_size,
        buy_volume_24h=buy_snap.volume_24h,
        sell_volume_24h=sell_snap.volume_24h,
        data_age_ms=data_age,
        confidence=confidence,
        timestamp=now,
    )
