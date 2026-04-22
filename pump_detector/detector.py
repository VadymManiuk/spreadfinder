"""
Pump/dump detector — runs periodically over recorded price history.

Inputs: PriceHistory, optional MarketCapFilter, PumpSettings.
Outputs: PumpAlert objects when a token's price moves more than the
         configured threshold over the configured window.
Assumptions:
  - Detection runs on a fixed interval (e.g. every 30s) from main.py.
  - Cooldowns are tracked per (base, direction) to prevent spam.
  - Volume and market cap filters are applied before alerting.
"""

from datetime import datetime, timezone
from decimal import Decimal

import structlog

from filters.market_cap_filter import MarketCapFilter
from pump_detector.models import PumpAlert
from pump_detector.price_history import PriceHistory, price_reference

logger = structlog.get_logger(__name__)


class PumpDetector:
    """
    Scans PriceHistory for tokens whose mid price has changed by more than
    `min_change_pct` over the last `window_minutes`.

    Picks the exchange showing the most extreme move (best signal).
    """

    def __init__(
        self,
        history: PriceHistory,
        min_change_pct: Decimal,
        window_minutes: int,
        min_volume_24h: Decimal,
        cooldown_seconds: int,
        mcap_filter: MarketCapFilter | None = None,
        min_market_cap: int = 0,
        max_market_cap: int = 500_000_000,
    ):
        self.history = history
        self.min_change_pct = Decimal(min_change_pct)
        self.window_seconds = window_minutes * 60
        self.min_volume_24h = Decimal(min_volume_24h)
        self.cooldown_seconds = cooldown_seconds
        self.mcap_filter = mcap_filter
        self.min_market_cap = min_market_cap
        self.max_market_cap = max_market_cap

        # (base, direction) -> last alert time
        self._last_alert: dict[tuple[str, str], datetime] = {}

    # ------------------------------------------------------------------
    # Runtime parameter updates (used by /pump telegram commands)
    # ------------------------------------------------------------------
    def update(
        self,
        min_change_pct: Decimal | None = None,
        window_minutes: int | None = None,
        min_volume_24h: Decimal | None = None,
    ) -> None:
        if min_change_pct is not None:
            self.min_change_pct = Decimal(min_change_pct)
        if window_minutes is not None:
            self.window_seconds = window_minutes * 60
        if min_volume_24h is not None:
            self.min_volume_24h = Decimal(min_volume_24h)

    # ------------------------------------------------------------------
    # Detection
    # ------------------------------------------------------------------
    def scan(self, now: datetime | None = None) -> list[PumpAlert]:
        """Scan all known tokens; return alerts that pass filters & cooldown."""
        ref = now or datetime.now(timezone.utc)
        alerts: list[PumpAlert] = []

        for base in self.history.known_bases():
            alert = self._check_base(base, ref)
            if alert is not None:
                alerts.append(alert)

        return alerts

    def _check_base(self, base: str, now: datetime) -> PumpAlert | None:
        """
        Inspect one base token across all its exchanges and return the most
        extreme price move that passes filters, or None.
        """
        latest_snaps = self.history.latest_snapshots_for_base(base)
        if not latest_snaps:
            return None

        # Filter by max 24h volume across exchanges
        max_volume = self._max_volume(latest_snaps)
        if max_volume is None or max_volume < self.min_volume_24h:
            return None

        # Market cap filter
        mcap_value: float | None = None
        if self.mcap_filter is not None:
            mcap_value = self.mcap_filter.get_mcap(base)
            if mcap_value is not None:
                if mcap_value > self.max_market_cap:
                    return None
                if mcap_value < self.min_market_cap:
                    return None

        # Find the exchange with the most extreme % change in the window
        best_change_pct = Decimal(0)
        best: tuple[Decimal, Decimal, datetime, datetime, int, str] | None = None

        for exchange in latest_snaps.keys():
            result = self.history.get_window_change(
                base, exchange, self.window_seconds, now
            )
            if result is None:
                continue
            start_price, current_price, start_ts, current_ts, actual_window = result

            # change_pct = (current - start) / start * 100
            if start_price == 0:
                continue
            change_pct = (current_price - start_price) / start_price * 100

            if abs(change_pct) > abs(best_change_pct):
                best_change_pct = change_pct
                best = (
                    start_price,
                    current_price,
                    start_ts,
                    current_ts,
                    actual_window,
                    exchange,
                )

        if best is None:
            return None

        if abs(best_change_pct) < self.min_change_pct:
            return None

        start_price, current_price, start_ts, current_ts, actual_window, ex = best
        direction = "pump" if best_change_pct > 0 else "dump"

        # Cooldown
        cooldown_key = (base, direction)
        last = self._last_alert.get(cooldown_key)
        if last is not None and (now - last).total_seconds() < self.cooldown_seconds:
            return None
        self._last_alert[cooldown_key] = now

        # Build per-exchange price/volume tables for the alert using the same
        # reference price source as the detector itself.
        exchange_prices: dict[str, Decimal] = {}
        exchange_volumes: dict[str, Decimal | None] = {}
        for ex_name, snap in latest_snaps.items():
            ref_price = price_reference(snap)
            if ref_price is None:
                continue
            exchange_prices[ex_name] = ref_price
            exchange_volumes[ex_name] = snap.volume_24h

        return PumpAlert(
            base=base,
            direction=direction,
            start_price=start_price,
            current_price=current_price,
            change_pct=best_change_pct,
            window_seconds=actual_window,
            start_ts=start_ts,
            current_ts=current_ts,
            max_volume_24h=max_volume,
            market_cap=mcap_value,
            exchange_prices=exchange_prices,
            exchange_volumes=exchange_volumes,
            triggered_on=ex,
            timestamp=now,
        )

    def _max_volume(self, snaps: dict) -> Decimal | None:
        """Highest 24h volume seen across all exchange snapshots for a token."""
        best: Decimal | None = None
        for snap in snaps.values():
            v = snap.volume_24h
            if v is None:
                continue
            if best is None or v > best:
                best = v
        return best
