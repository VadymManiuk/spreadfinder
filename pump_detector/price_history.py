"""
Rolling per-exchange price history for pump/dump detection.

Inputs: MarketSnapshot objects fed via record().
Outputs: Time-windowed price lookups and bounded-history diagnostics.
Assumptions:
  - Stored in-memory only; resets on restart.
  - Samples are throttled and capped per token+exchange series.
  - Retention and cadence adapt to the configured pump window.
  - Perp reference prices are stored instead of raw book mid:
    prefer mark price, then index price.
  - DEX aggregator sources are excluded from pump/dump history because
    they are useful for spread discovery but too noisy as standalone triggers.
"""

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from math import ceil

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


@dataclass(frozen=True)
class PriceHistoryStats:
    """Current pump-history size, limits, and cumulative throttle count."""

    series_count: int
    sample_count: int
    throttled_samples: int
    retention_minutes: int
    sample_interval_seconds: int
    max_samples_per_series: int


class PriceHistory:
    """
    Rolling price history keyed by (base_token, exchange).

    Each key holds a deque of (timestamp, reference_price) samples.
    """

    def __init__(
        self,
        retention_minutes: int = 90,
        sample_interval_seconds: int = 10,
        max_samples_per_series: int = 600,
        window_buffer_minutes: int = 30,
    ):
        if retention_minutes <= 0:
            raise ValueError("retention_minutes must be positive")
        if sample_interval_seconds <= 0:
            raise ValueError("sample_interval_seconds must be positive")
        if max_samples_per_series < 2:
            raise ValueError("max_samples_per_series must be at least 2")
        if window_buffer_minutes < 0:
            raise ValueError("window_buffer_minutes cannot be negative")

        self._base_retention_minutes = retention_minutes
        self._base_sample_interval_seconds = sample_interval_seconds
        self._max_samples_per_series = max_samples_per_series
        self._window_buffer_minutes = window_buffer_minutes
        self._retention = timedelta(minutes=retention_minutes)
        self._sample_interval = timedelta(seconds=sample_interval_seconds)
        self._throttled_samples = 0

        # (base, exchange) -> deque of (timestamp, reference_price)
        self._data: dict[tuple[str, str], deque[tuple[datetime, Decimal]]] = {}
        # base -> latest snapshot per exchange (for alert enrichment)
        self._latest_by_base: dict[str, dict[str, MarketSnapshot]] = {}

    def configure_window(self, window_minutes: int) -> None:
        """
        Resize retention and sampling cadence for a pump detection window.

        retention_minutes = max(configured retention, window + buffer)
        sample_interval_seconds = max(
            configured interval,
            ceil(retention_seconds / (max_samples - 1)),
        )

        The adaptive cadence preserves long windows without ever allowing a
        series to exceed the configured hard cap.
        """
        if window_minutes <= 0:
            raise ValueError("window_minutes must be positive")

        retention_minutes = max(
            self._base_retention_minutes,
            window_minutes + self._window_buffer_minutes,
        )
        retention_seconds = retention_minutes * 60
        adaptive_interval_seconds = ceil(
            retention_seconds / (self._max_samples_per_series - 1)
        )

        self._retention = timedelta(minutes=retention_minutes)
        self._sample_interval = timedelta(
            seconds=max(
                self._base_sample_interval_seconds,
                adaptive_interval_seconds,
            )
        )
        self._trim_all()

    def record(self, base: str, snapshot: MarketSnapshot) -> None:
        """
        Update the latest snapshot and periodically append a history sample.

        Latest snapshots remain live on every valid tick. Only the historical
        sample deque is throttled.
        """
        if snapshot.bid <= 0 or snapshot.ask <= 0:
            return

        reference_price = price_reference(snapshot)
        if reference_price is None or reference_price <= 0:
            return

        key = (base, snapshot.exchange)
        self._latest_by_base.setdefault(base, {})[snapshot.exchange] = snapshot

        dq = self._data.setdefault(
            key,
            deque(maxlen=self._max_samples_per_series),
        )
        if dq and snapshot.local_ts - dq[-1][0] < self._sample_interval:
            self._throttled_samples += 1
            return

        dq.append((snapshot.local_ts, reference_price))
        self._trim(dq, snapshot.local_ts)

    def _trim(
        self,
        dq: deque[tuple[datetime, Decimal]],
        now: datetime,
    ) -> None:
        """Drop samples older than the retention window."""
        cutoff = now - self._retention
        while dq and dq[0][0] < cutoff:
            dq.popleft()

    def _trim_all(self) -> None:
        """Apply the current retention window to every populated series."""
        for dq in self._data.values():
            if dq:
                self._trim(dq, dq[-1][0])

    def prune(self, now: datetime | None = None) -> None:
        """Remove expired samples and inactive token+exchange series."""
        ref = now or datetime.now(timezone.utc)
        cutoff = ref - self._retention

        for key, dq in list(self._data.items()):
            self._trim(dq, ref)
            if dq:
                continue

            del self._data[key]
            base, exchange = key
            latest_by_exchange = self._latest_by_base.get(base)
            if latest_by_exchange is None:
                continue

            latest = latest_by_exchange.get(exchange)
            if latest is None or latest.local_ts < cutoff:
                latest_by_exchange.pop(exchange, None)
            if not latest_by_exchange:
                self._latest_by_base.pop(base, None)

    def clear(self) -> None:
        """Release all history and latest pump snapshots immediately."""
        self._data.clear()
        self._latest_by_base.clear()

    def stats(self) -> PriceHistoryStats:
        """Return current bounded-history metrics for diagnostics."""
        return PriceHistoryStats(
            series_count=len(self._data),
            sample_count=sum(len(dq) for dq in self._data.values()),
            throttled_samples=self._throttled_samples,
            retention_minutes=int(self._retention.total_seconds() // 60),
            sample_interval_seconds=int(self._sample_interval.total_seconds()),
            max_samples_per_series=self._max_samples_per_series,
        )

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
