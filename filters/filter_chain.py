"""
Filter chain that runs all configured filters on a spread opportunity.

Inputs: SpreadOpportunity, filter configuration.
Outputs: (pass: bool, list of FilterResult).
Assumptions:
  - Filters run in order; chain short-circuits on first rejection.
  - Cooldown and persistence filters are stateful; others are pure functions.
"""

from decimal import Decimal

import structlog

from models.snapshot import SpreadOpportunity
from filters.opportunity_filters import (
    FilterResult,
    CooldownFilter,
    PersistenceFilter,
    check_min_gross_spread,
    check_max_gross_spread,
    check_min_net_spread,
    check_min_bid_size,
    check_min_ask_size,
    check_min_volume,
    check_max_data_age,
    check_min_confidence,
)

logger = structlog.get_logger(__name__)


class FilterChain:
    """
    Runs all configured filters on a SpreadOpportunity.

    Usage:
        chain = FilterChain(
            min_gross_spread_bps=Decimal("10"),
            min_net_spread_bps=Decimal("5"),
            ...
        )
        passed, results = chain.evaluate(opportunity)
        if passed:
            chain.record_alert(opportunity)
    """

    def __init__(
        self,
        min_gross_spread_bps: Decimal = Decimal("10.0"),
        max_gross_spread_bps: Decimal = Decimal("500.0"),
        min_net_spread_bps: Decimal = Decimal("5.0"),
        min_bid_size: Decimal = Decimal("100.0"),
        min_ask_size: Decimal = Decimal("100.0"),
        min_volume_24h: Decimal | None = None,
        max_data_age_ms: int = 2000,
        min_confidence: Decimal = Decimal("0.3"),
        cooldown_seconds: int = 300,
        persistence_ms: int = 1000,
    ):
        self.min_gross_spread_bps = min_gross_spread_bps
        self.max_gross_spread_bps = max_gross_spread_bps
        self.min_net_spread_bps = min_net_spread_bps
        self.min_bid_size = min_bid_size
        self.min_ask_size = min_ask_size
        self.min_volume_24h = min_volume_24h
        self.max_data_age_ms = max_data_age_ms
        self.min_confidence = min_confidence

        self._cooldown = CooldownFilter(cooldown_seconds)
        self._persistence = PersistenceFilter(persistence_ms)

    def evaluate(self, opp: SpreadOpportunity) -> tuple[bool, list[FilterResult]]:
        """
        Run all filters on the opportunity. Short-circuits on first failure.

        Returns:
            (passed, results): passed is True if all filters pass.
            results contains all checked FilterResults (including the failing one).
        """
        results: list[FilterResult] = []

        checks = [
            lambda: check_min_gross_spread(opp, self.min_gross_spread_bps),
            lambda: check_max_gross_spread(opp, self.max_gross_spread_bps),
            lambda: check_min_net_spread(opp, self.min_net_spread_bps),
            lambda: check_min_bid_size(opp, self.min_bid_size),
            lambda: check_min_ask_size(opp, self.min_ask_size),
            lambda: check_min_volume(opp, self.min_volume_24h),
            lambda: check_max_data_age(opp, self.max_data_age_ms),
            lambda: check_min_confidence(opp, self.min_confidence),
            lambda: self._cooldown.check(opp),
            lambda: self._persistence.check(opp),
        ]

        for check in checks:
            result = check()
            results.append(result)
            if not result:
                logger.debug(
                    "filter_rejected",
                    symbol=opp.canonical_symbol,
                    filter=result.filter_name,
                    reason=result.reason,
                )
                return False, results

        return True, results

    def record_alert(self, opp: SpreadOpportunity) -> None:
        """Record that an alert was sent (updates cooldown timer)."""
        self._cooldown.record_alert(opp)

    def remove_persistence(self, opp: SpreadOpportunity) -> None:
        """Remove persistence tracking for a spread that disappeared."""
        self._persistence.remove(opp)

    def clear_state(self) -> None:
        """Clear all stateful filter data (cooldowns, persistence)."""
        self._cooldown.clear()
        self._persistence.clear()
