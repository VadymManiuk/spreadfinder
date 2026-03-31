"""
Normalized market data models for the spread scanner.

Inputs: Raw exchange data (prices, sizes, timestamps, funding rates).
Outputs: Immutable, validated MarketSnapshot and SpreadOpportunity objects.
Assumptions: All price/size fields use Decimal for financial accuracy.
"""

from datetime import datetime, timezone
from decimal import Decimal

from pydantic import BaseModel, Field, computed_field


class MarketSnapshot(BaseModel, frozen=True):
    """
    A single point-in-time view of one perpetual futures market on one exchange.

    Every exchange adapter normalizes its raw data into this schema before
    passing it downstream to the spread engine.
    """

    # Canonical symbol in {BASE}-{QUOTE}-PERP format (e.g. "BTC-USDT-PERP")
    canonical_symbol: str

    # Exchange identifier (e.g. "binance", "hyperliquid", "gate")
    exchange: str

    # Top-of-book prices and sizes
    bid: Decimal
    ask: Decimal
    bid_size: Decimal
    ask_size: Decimal

    # Timestamps
    exchange_ts: datetime | None = None
    local_ts: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Optional enrichment fields
    mark_price: Decimal | None = None
    index_price: Decimal | None = None
    funding_rate: Decimal | None = None
    volume_24h: Decimal | None = None

    # Staleness flag — set by adapter when no update received within threshold
    is_stale: bool = False

    @computed_field  # type: ignore[prop-decorator]
    @property
    def mid_price(self) -> Decimal:
        """Midpoint of best bid and ask."""
        # mid_price = (bid + ask) / 2
        return (self.bid + self.ask) / 2

    @computed_field  # type: ignore[prop-decorator]
    @property
    def spread_bps(self) -> Decimal:
        """Bid-ask spread in basis points (internal spread, not cross-exchange)."""
        if self.ask == 0:
            return Decimal(0)
        # spread_bps = ((ask - bid) / ask) * 10000
        return ((self.ask - self.bid) / self.ask) * 10000

    def data_age_ms(self, now: datetime | None = None) -> int:
        """
        Milliseconds since local_ts. Pass `now` for deterministic testing.
        """
        ref = now or datetime.now(timezone.utc)
        delta = ref - self.local_ts
        return int(delta.total_seconds() * 1000)


class SpreadOpportunity(BaseModel, frozen=True):
    """
    A detected cross-exchange spread opportunity, ready for filtering and alerting.

    Represents one direction: buy on buy_exchange at buy_ask, sell on
    sell_exchange at sell_bid.
    """

    # What asset
    canonical_symbol: str

    # Direction: buy here, sell there
    buy_exchange: str
    sell_exchange: str
    buy_ask: Decimal
    sell_bid: Decimal

    # Spread metrics
    gross_spread: Decimal       # sell_bid - buy_ask
    gross_spread_bps: Decimal   # (gross_spread / buy_ask) * 10000
    net_spread: Decimal         # gross_spread - estimated_fees - estimated_slippage
    net_spread_bps: Decimal     # (net_spread / buy_ask) * 10000

    # Cost estimates  # ESTIMATE — refine later
    estimated_fees: Decimal
    estimated_slippage: Decimal

    # Funding rates (if available)
    buy_funding_rate: Decimal | None = None
    sell_funding_rate: Decimal | None = None

    # Liquidity context
    buy_ask_size: Decimal
    sell_bid_size: Decimal
    buy_volume_24h: Decimal | None = None
    sell_volume_24h: Decimal | None = None

    # Quality metrics
    data_age_ms: int            # max age of the two snapshots
    confidence: Decimal         # 0.0 to 1.0

    # When this opportunity was detected
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
