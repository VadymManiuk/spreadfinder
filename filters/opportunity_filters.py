"""
Individual filter implementations for spread opportunities.

Inputs: A SpreadOpportunity object.
Outputs: (pass: bool, reason: str | None) — reason is set on rejection.
Assumptions:
  - Each filter checks one quality dimension.
  - Filters are stateless except CooldownFilter which tracks last alert times.
"""

import time
from decimal import Decimal

from models.snapshot import SpreadOpportunity


class FilterResult:
    """Result of a single filter check."""

    __slots__ = ("passed", "reason", "filter_name")

    def __init__(self, passed: bool, filter_name: str, reason: str | None = None):
        self.passed = passed
        self.filter_name = filter_name
        self.reason = reason

    def __bool__(self) -> bool:
        return self.passed


def check_min_gross_spread(opp: SpreadOpportunity, min_bps: Decimal) -> FilterResult:
    """Reject if gross spread is below minimum threshold."""
    if opp.gross_spread_bps < min_bps:
        return FilterResult(
            False, "min_gross_spread",
            f"gross_spread_bps={opp.gross_spread_bps:.1f} < {min_bps}",
        )
    return FilterResult(True, "min_gross_spread")


def check_min_net_spread(opp: SpreadOpportunity, min_bps: Decimal) -> FilterResult:
    """Reject if net spread (after costs) is below minimum threshold."""
    if opp.net_spread_bps < min_bps:
        return FilterResult(
            False, "min_net_spread",
            f"net_spread_bps={opp.net_spread_bps:.1f} < {min_bps}",
        )
    return FilterResult(True, "min_net_spread")


def check_min_bid_size(opp: SpreadOpportunity, min_size: Decimal) -> FilterResult:
    """Reject if sell-side bid size is too small."""
    if opp.sell_bid_size < min_size:
        return FilterResult(
            False, "min_bid_size",
            f"sell_bid_size={opp.sell_bid_size} < {min_size}",
        )
    return FilterResult(True, "min_bid_size")


def check_min_ask_size(opp: SpreadOpportunity, min_size: Decimal) -> FilterResult:
    """Reject if buy-side ask size is too small."""
    if opp.buy_ask_size < min_size:
        return FilterResult(
            False, "min_ask_size",
            f"buy_ask_size={opp.buy_ask_size} < {min_size}",
        )
    return FilterResult(True, "min_ask_size")


def check_min_volume(opp: SpreadOpportunity, min_volume: Decimal | None) -> FilterResult:
    """Reject if 24h volume on either side is below threshold. Skip if threshold is None."""
    if min_volume is None:
        return FilterResult(True, "min_volume")

    if opp.buy_volume_24h is not None and opp.buy_volume_24h < min_volume:
        return FilterResult(
            False, "min_volume",
            f"buy_volume_24h={opp.buy_volume_24h} < {min_volume}",
        )
    if opp.sell_volume_24h is not None and opp.sell_volume_24h < min_volume:
        return FilterResult(
            False, "min_volume",
            f"sell_volume_24h={opp.sell_volume_24h} < {min_volume}",
        )
    return FilterResult(True, "min_volume")


def check_max_data_age(opp: SpreadOpportunity, max_age_ms: int) -> FilterResult:
    """Reject if data is too old."""
    if opp.data_age_ms > max_age_ms:
        return FilterResult(
            False, "max_data_age",
            f"data_age_ms={opp.data_age_ms} > {max_age_ms}",
        )
    return FilterResult(True, "max_data_age")


def check_min_confidence(opp: SpreadOpportunity, min_conf: Decimal) -> FilterResult:
    """Reject if confidence score is below threshold."""
    if opp.confidence < min_conf:
        return FilterResult(
            False, "min_confidence",
            f"confidence={opp.confidence:.2f} < {min_conf}",
        )
    return FilterResult(True, "min_confidence")


class CooldownFilter:
    """
    Prevents alert spam by enforcing a cooldown per symbol+exchange pair.

    Key format: "{symbol}:{buy_exchange}:{sell_exchange}"
    """

    def __init__(self, cooldown_seconds: int):
        self.cooldown_seconds = cooldown_seconds
        # {key: last_alert_timestamp}
        self._last_alert: dict[str, float] = {}

    def _make_key(self, opp: SpreadOpportunity) -> str:
        return f"{opp.canonical_symbol}:{opp.buy_exchange}:{opp.sell_exchange}"

    def check(self, opp: SpreadOpportunity) -> FilterResult:
        """Reject if the same pair was alerted within cooldown period."""
        key = self._make_key(opp)
        now = time.monotonic()
        last = self._last_alert.get(key)

        if last is not None and (now - last) < self.cooldown_seconds:
            remaining = int(self.cooldown_seconds - (now - last))
            return FilterResult(
                False, "cooldown",
                f"cooldown active for {key}, {remaining}s remaining",
            )
        return FilterResult(True, "cooldown")

    def record_alert(self, opp: SpreadOpportunity) -> None:
        """Record that an alert was sent for this opportunity."""
        key = self._make_key(opp)
        self._last_alert[key] = time.monotonic()

    def clear(self) -> None:
        """Clear all cooldown state."""
        self._last_alert.clear()


class PersistenceFilter:
    """
    Requires a spread to persist for a minimum duration before alerting.

    Tracks first-seen time per symbol+direction. Resets if the spread disappears.
    """

    def __init__(self, persistence_ms: int):
        self.persistence_ms = persistence_ms
        # {key: first_seen_monotonic_ms}
        self._first_seen: dict[str, float] = {}

    def _make_key(self, opp: SpreadOpportunity) -> str:
        return f"{opp.canonical_symbol}:{opp.buy_exchange}:{opp.sell_exchange}"

    def check(self, opp: SpreadOpportunity) -> FilterResult:
        """Reject if the spread hasn't persisted long enough."""
        if self.persistence_ms <= 0:
            return FilterResult(True, "persistence")

        key = self._make_key(opp)
        now_ms = time.monotonic() * 1000

        if key not in self._first_seen:
            self._first_seen[key] = now_ms
            return FilterResult(
                False, "persistence",
                f"first seen, need {self.persistence_ms}ms persistence",
            )

        elapsed = now_ms - self._first_seen[key]
        if elapsed < self.persistence_ms:
            remaining = int(self.persistence_ms - elapsed)
            return FilterResult(
                False, "persistence",
                f"seen for {int(elapsed)}ms, need {self.persistence_ms}ms ({remaining}ms remaining)",
            )

        return FilterResult(True, "persistence")

    def remove(self, opp: SpreadOpportunity) -> None:
        """Remove tracking for a spread that no longer exists."""
        key = self._make_key(opp)
        self._first_seen.pop(key, None)

    def clear(self) -> None:
        """Clear all persistence state."""
        self._first_seen.clear()
