"""
Rolling per-exchange price history for pump/dump detection.

Inputs: MarketSnapshot objects fed via record().
Outputs: Time-windowed price lookups via get_oldest_within().
Assumptions:
  - Stored in-memory only; resets on restart.
  - Trimmed on every insert to bounded retention window.
  - Perp reference prices are stored instead of raw book mid:
    prefer mark price, then index price.
  - DEX aggregator sources are excluded from pump/dump history because
    they are useful for spread discovery but too noisy as standalone triggers.
"""

from collections import deque
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from models.snapshot import MarketSnapshot
from utils.venues import is_dex_exchange


def price_reference(snapshot: MarketSnapshot) -> Decimal | None:
    """
    Choose the price used for pump/dump history on this snapshot.

    Futures venues use mark/index because book mid can jump on thin books
    without reflecting a real traded or fair price.
    DEX sources are ignored entirely for pump/dump detection.
    """
    if is_dex_exchange(snapshot.exchange):
        return None

    if snapshot.mark_price is not None and snapshot.mark_price > 0:
        return snapshot.mark_price
    if snapshot.index_price is not None and snapshot.index_price > 0:
        return snapshot.index_price
    return None


class PriceHistory:
    """
    Rolling price history keyed by (base_token, exchange).

    Each key holds a deque of (timestamp, reference_price) samples.
    """

    def __init__(self, retention_minutes: int = 180):
        self._retention = timedelta(minutes=retention_minutes)
        # (base, exchange) -> deque of (ts, mid_price)
        self._data: dict[tuple[str, str], deque[tuple[datetime, Decimal]]] = {}
        # base -> latest snapshot per exchange (for alert enrichment)
        self._latest_by_base: dict[str, dict[str, MarketSnapshot]] = {}

    def record(self, base: str, snapshot: MarketSnapshot) -> None:
        """Append a new reference-price sample for (base, exchange)."""
        if snapshot.bid <= 0 or snapshot.ask <= 0:
            return

        reference_price = price_reference(snapshot)
        if reference_price is None or reference_price <= 0:
            return

        key = (base, snapshot.exchange)

        dq = self._data.setdefault(key, deque())
        dq.append((snapshot.local_ts, reference_price))
        self._trim(dq, snapshot.local_ts)

        self._latest_by_base.setdefault(base, {})[snapshot.exchange] = snapshot

    def _trim(self, dq: deque, now: datetime) -> None:
        """Drop samples older than the retention window."""
        cutoff = now - self._retention
        while dq and dq[0][0] < cutoff:
            dq.popleft()

    def get_window_change(
        self,
        base: str,
        exchange: str,
        window_seconds: int,
        now: datetime | None = None,
    ) -> tuple[Decimal, Decimal, datetime, datetime, int] | None:
        """
        Compare the latest mid price to the oldest sample within the window.

        Returns (start_price, current_price, start_ts, current_ts, actual_window_s)
        or None if no samples or insufficient history.
        """
        key = (base, exchange)
        dq = self._data.get(key)
        if not dq or len(dq) < 2:
            return None

        ref = now or datetime.now(timezone.utc)
        cutoff = ref - timedelta(seconds=window_seconds)

        # Latest sample
        current_ts, current_price = dq[-1]

        # Find the oldest sample that is still within the window.
        # We iterate from the start; deque is small after trimming so this is fine.
        start_ts, start_price = None, None
        for ts, price in dq:
            if ts >= cutoff:
                start_ts, start_price = ts, price
                break

        if start_ts is None or start_price is None or start_price == 0:
            return None

        actual_window = int((current_ts - start_ts).total_seconds())
        if actual_window <= 0:
            return None

        return start_price, current_price, start_ts, current_ts, actual_window

    def latest_snapshots_for_base(self, base: str) -> dict[str, MarketSnapshot]:
        """All most-recent snapshots per exchange for a base token."""
        return dict(self._latest_by_base.get(base, {}))

    def known_bases(self) -> list[str]:
        """All base tokens that have at least one recorded sample."""
        return list(self._latest_by_base.keys())
