"""
Pump alert data model.

Inputs: Detected price-change events from PumpDetector.
Outputs: Immutable PumpAlert records ready for formatting + Telegram dispatch.
Assumptions:
  - All prices stored as Decimal for accuracy.
  - direction is "pump" (price up) or "dump" (price down).
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field


class PumpAlert(BaseModel, frozen=True):
    """A detected pump or dump on a single base token."""

    base: str                                # canonical base token (e.g. "ARIA")
    direction: Literal["pump", "dump"]       # price direction

    # Price movement
    start_price: Decimal                     # price at the start of the window
    current_price: Decimal                   # latest observed price
    change_pct: Decimal                      # signed % change (current vs start)

    # Window
    window_seconds: int                      # actual window length used
    start_ts: datetime                       # time of the earliest sample used
    current_ts: datetime                     # time of the latest sample used

    # Liquidity context
    max_volume_24h: Decimal | None = None    # best 24h volume across exchanges
    market_cap: float | None = None          # USD, if known

    # Per-exchange snapshot of latest price (for the alert table)
    # {exchange: latest_price}
    exchange_prices: dict[str, Decimal] = Field(default_factory=dict)
    # {exchange: latest_volume_24h}
    exchange_volumes: dict[str, Decimal | None] = Field(default_factory=dict)

    # Reference exchange that triggered the alert
    triggered_on: str

    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
