"""
Confidence scoring for spread opportunities.

Inputs: Two MarketSnapshot objects, gross spread in bps.
Outputs: Confidence score between 0.0 and 1.0.
Assumptions:
  - Weights are tuned for small-cap tokens (<$200M mcap).
  - Thresholds will need refinement with live data.
  - Score is a weighted average of four factors.
"""

from datetime import datetime, timezone
from decimal import Decimal

from models.snapshot import MarketSnapshot

# Confidence factor weights (must sum to 1.0)
WEIGHT_FRESHNESS = Decimal("0.30")
WEIGHT_LIQUIDITY = Decimal("0.30")
WEIGHT_VOLUME = Decimal("0.20")
WEIGHT_SPREAD_MAG = Decimal("0.20")

# Thresholds — calibrated for small-cap perps
# ESTIMATE — refine with live data
MAX_ACCEPTABLE_AGE_MS = 2000       # beyond this, freshness score = 0
PENALTY_AGE_MS = 500               # above this, freshness starts degrading

MIN_SIZE_USD = Decimal("500")      # min top-of-book size (in quote) for full score
LOW_SIZE_USD = Decimal("100")      # below this, liquidity score = 0

# Volume thresholds for small caps
MIN_VOLUME_24H = Decimal("500000")    # $500K — full score
LOW_VOLUME_24H = Decimal("100000")    # $100K — score starts degrading

# Spread magnitude — larger spreads on small caps are more reliable
MIN_SPREAD_BPS = Decimal("5")      # below this, magnitude score = 0
GOOD_SPREAD_BPS = Decimal("20")    # at or above this, full score


def score_freshness(snap_a: MarketSnapshot, snap_b: MarketSnapshot, now: datetime | None = None) -> Decimal:
    """
    Score based on data age of the older snapshot.

    freshness = 1.0 if age <= PENALTY_AGE_MS
    freshness = linear decay to 0.0 at MAX_ACCEPTABLE_AGE_MS
    freshness = 0.0 if age > MAX_ACCEPTABLE_AGE_MS
    """
    ref = now or datetime.now(timezone.utc)
    age = max(snap_a.data_age_ms(ref), snap_b.data_age_ms(ref))

    if age <= PENALTY_AGE_MS:
        return Decimal("1.0")
    if age >= MAX_ACCEPTABLE_AGE_MS:
        return Decimal("0.0")

    # Linear interpolation between PENALTY_AGE_MS and MAX_ACCEPTABLE_AGE_MS
    # score = 1.0 - (age - PENALTY) / (MAX - PENALTY)
    ratio = Decimal(age - PENALTY_AGE_MS) / Decimal(MAX_ACCEPTABLE_AGE_MS - PENALTY_AGE_MS)
    return Decimal("1.0") - ratio


def score_liquidity(buy_ask_size: Decimal, sell_bid_size: Decimal) -> Decimal:
    """
    Score based on the smaller of buy ask size and sell bid size (in quote currency).

    liquidity = 0.0 if min_size < LOW_SIZE_USD
    liquidity = linear scale to 1.0 at MIN_SIZE_USD
    liquidity = 1.0 if min_size >= MIN_SIZE_USD
    """
    min_size = min(buy_ask_size, sell_bid_size)

    if min_size >= MIN_SIZE_USD:
        return Decimal("1.0")
    if min_size <= LOW_SIZE_USD:
        return Decimal("0.0")

    # score = (min_size - LOW) / (MIN - LOW)
    return (min_size - LOW_SIZE_USD) / (MIN_SIZE_USD - LOW_SIZE_USD)


def score_volume(volume_a: Decimal | None, volume_b: Decimal | None) -> Decimal:
    """
    Score based on the lower 24h volume of the two exchanges.

    Unknown volume → 0.5 (neutral, not penalized harshly).
    """
    if volume_a is None or volume_b is None:
        return Decimal("0.5")

    min_vol = min(volume_a, volume_b)

    if min_vol >= MIN_VOLUME_24H:
        return Decimal("1.0")
    if min_vol <= LOW_VOLUME_24H:
        return Decimal("0.0")

    # score = (min_vol - LOW) / (MIN - LOW)
    return (min_vol - LOW_VOLUME_24H) / (MIN_VOLUME_24H - LOW_VOLUME_24H)


def score_spread_magnitude(gross_spread_bps: Decimal) -> Decimal:
    """
    Score based on how large the gross spread is.

    Larger spreads on small caps are more likely to be real opportunities.
    """
    if gross_spread_bps >= GOOD_SPREAD_BPS:
        return Decimal("1.0")
    if gross_spread_bps <= MIN_SPREAD_BPS:
        return Decimal("0.0")

    # score = (spread - MIN) / (GOOD - MIN)
    return (gross_spread_bps - MIN_SPREAD_BPS) / (GOOD_SPREAD_BPS - MIN_SPREAD_BPS)


def calculate_confidence(
    snap_a: MarketSnapshot,
    snap_b: MarketSnapshot,
    buy_ask_size: Decimal,
    sell_bid_size: Decimal,
    gross_spread_bps: Decimal,
    now: datetime | None = None,
) -> Decimal:
    """
    Overall confidence score: weighted average of four factors.

    confidence = freshness * 0.30 + liquidity * 0.30 + volume * 0.20 + magnitude * 0.20
    """
    freshness = score_freshness(snap_a, snap_b, now)
    liquidity = score_liquidity(buy_ask_size, sell_bid_size)
    volume = score_volume(snap_a.volume_24h, snap_b.volume_24h)
    magnitude = score_spread_magnitude(gross_spread_bps)

    # confidence = w1*freshness + w2*liquidity + w3*volume + w4*magnitude
    score = (
        WEIGHT_FRESHNESS * freshness
        + WEIGHT_LIQUIDITY * liquidity
        + WEIGHT_VOLUME * volume
        + WEIGHT_SPREAD_MAG * magnitude
    )

    # Clamp to [0.0, 1.0]
    return max(Decimal("0.0"), min(Decimal("1.0"), score))
