"""
Reconnection helper with exponential backoff.

Inputs: Base delay, max delay, optional jitter.
Outputs: Async context for retry loops with increasing wait times.
Assumptions: Used by exchange adapters for WebSocket reconnection.
"""

import asyncio
import random

import structlog

logger = structlog.get_logger(__name__)


class ExponentialBackoff:
    """
    Tracks retry state with exponential backoff and optional jitter.

    Usage:
        backoff = ExponentialBackoff()
        while True:
            try:
                await connect()
                backoff.reset()
            except ConnectionError:
                await backoff.wait()
    """

    def __init__(
        self,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        jitter: float = 0.5,
    ):
        """
        Args:
            base_delay: Initial wait time in seconds.
            max_delay: Cap on wait time in seconds.
            jitter: Random factor (0.0 = no jitter, 1.0 = up to 100% extra).
        """
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter = jitter
        self._attempt = 0

    @property
    def attempt(self) -> int:
        return self._attempt

    def reset(self) -> None:
        """Reset attempt counter after a successful connection."""
        self._attempt = 0

    def _next_delay(self) -> float:
        """
        Calculate delay for next retry.

        delay = min(base_delay * 2^attempt, max_delay) + random jitter
        """
        # delay = min(base * 2^attempt, max)
        delay = min(self.base_delay * (2 ** self._attempt), self.max_delay)
        if self.jitter > 0:
            delay += delay * random.uniform(0, self.jitter)
        return delay

    async def wait(self) -> float:
        """
        Sleep for the current backoff duration and increment attempt counter.

        Returns:
            The actual delay slept in seconds.
        """
        delay = self._next_delay()
        self._attempt += 1
        logger.info(
            "backoff_waiting",
            delay_seconds=round(delay, 2),
            attempt=self._attempt,
        )
        await asyncio.sleep(delay)
        return delay
