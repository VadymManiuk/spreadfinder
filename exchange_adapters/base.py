"""
Abstract base class for exchange WebSocket adapters.

Inputs: Exchange configuration, symbol list, snapshot callback.
Outputs: Normalized MarketSnapshot objects via callback.
Assumptions:
  - Each adapter manages its own WebSocket connection lifecycle.
  - Reconnection is handled automatically with exponential backoff.
  - Stale feed detection triggers reconnect when no data arrives within threshold.
"""

import abc
import asyncio
from collections.abc import Callable, Coroutine
from datetime import datetime, timezone
from typing import Any

import structlog

from models.snapshot import MarketSnapshot
from utils.reconnect import ExponentialBackoff

logger = structlog.get_logger(__name__)

# Type alias for the callback that receives new snapshots
SnapshotCallback = Callable[[MarketSnapshot], Coroutine[Any, Any, None]]


class BaseExchangeAdapter(abc.ABC):
    """
    Base class all exchange adapters inherit from.

    Provides:
      - Connection lifecycle management (connect, disconnect, reconnect)
      - Stale feed detection with configurable threshold
      - Heartbeat tracking
      - Exponential backoff on reconnect

    Subclasses must implement:
      - _connect(): Establish WebSocket connection
      - _disconnect(): Close WebSocket connection
      - _subscribe(): Send subscription messages
      - _listen(): Main receive loop
      - _build_ws_url(): Construct WebSocket URL from subscribed symbols
    """

    def __init__(
        self,
        exchange_name: str,
        on_snapshot: SnapshotCallback,
        stale_threshold_seconds: float = 10.0,
        heartbeat_interval_seconds: float = 30.0,
    ):
        self.exchange_name = exchange_name
        self.on_snapshot = on_snapshot
        self.stale_threshold_seconds = stale_threshold_seconds
        self.heartbeat_interval_seconds = heartbeat_interval_seconds

        self._backoff = ExponentialBackoff()
        self._running = False
        self._last_message_time: datetime | None = None
        self._tasks: list[asyncio.Task] = []
        self._log = logger.bind(exchange=exchange_name)

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def last_message_time(self) -> datetime | None:
        return self._last_message_time

    def _update_heartbeat(self) -> None:
        """Record that a message was received (for stale detection)."""
        self._last_message_time = datetime.now(timezone.utc)

    def _is_stale(self) -> bool:
        """Check if the feed is stale (no message within threshold)."""
        if self._last_message_time is None:
            return False
        age = (datetime.now(timezone.utc) - self._last_message_time).total_seconds()
        return age > self.stale_threshold_seconds

    async def start(self) -> None:
        """Start the adapter: connect, subscribe, and begin listening."""
        self._running = True
        self._log.info("adapter_starting")

        while self._running:
            try:
                await self._connect()
                self._backoff.reset()
                await self._subscribe()
                self._update_heartbeat()

                # Run listener and stale checker concurrently
                listen_task = asyncio.create_task(self._listen())
                stale_task = asyncio.create_task(self._stale_checker())
                self._tasks = [listen_task, stale_task]

                # Wait until one of them exits (error or stale detection).
                task_error = await self._wait_until_first_task_finishes(self._tasks)
                if task_error:
                    self._log.error(
                        "task_error",
                        error_type=type(task_error).__name__,
                        error=str(task_error),
                    )

            except asyncio.CancelledError:
                self._log.info("adapter_cancelled")
                break
            except Exception:
                self._log.exception("adapter_error")

            finally:
                self._tasks = []
                await self._safe_disconnect()

            if self._running:
                await self._backoff.wait()

        self._log.info("adapter_stopped")

    async def stop(self) -> None:
        """Gracefully stop the adapter."""
        self._log.info("adapter_stopping")
        self._running = False
        await self._cancel_tasks(self._tasks)
        self._tasks = []
        await self._safe_disconnect()

    async def _safe_disconnect(self) -> None:
        """Disconnect, swallowing any errors."""
        try:
            await self._disconnect()
        except Exception:
            self._log.debug("disconnect_error", exc_info=True)

    async def _stale_checker(self) -> None:
        """Periodically check for stale feeds and force reconnect if detected."""
        while self._running:
            await asyncio.sleep(self.heartbeat_interval_seconds)
            if self._is_stale():
                self._log.warning(
                    "stale_feed_detected",
                    last_message_age_seconds=round(
                        (datetime.now(timezone.utc) - self._last_message_time).total_seconds(), 1
                    )
                    if self._last_message_time
                    else None,
                )
                return  # exit to trigger reconnect in start()

    async def _cancel_tasks(self, tasks: list[asyncio.Task]) -> None:
        """Cancel tasks and await them so exceptions don't leak."""
        for task in tasks:
            if not task.done():
                task.cancel()

        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                self._log.debug("task_cancel_error", exc_info=True)

    async def _wait_until_first_task_finishes(
        self,
        tasks: list[asyncio.Task],
    ) -> Exception | None:
        """
        Wait until the first task finishes and consume every task result.

        This prevents "Task exception was never retrieved" warnings when
        multiple listener tasks fail around the same time during reconnects.
        """
        if not tasks:
            return None

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        first_error: Exception | None = None

        for task in done:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                if first_error is None:
                    first_error = exc

        pending_list = list(pending)
        for task in pending_list:
            if not task.done():
                task.cancel()

        for task in pending_list:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                if first_error is None:
                    first_error = exc

        return first_error

    # ---- Abstract methods subclasses must implement ----

    @abc.abstractmethod
    async def _connect(self) -> None:
        """Establish WebSocket connection."""

    @abc.abstractmethod
    async def _disconnect(self) -> None:
        """Close WebSocket connection."""

    @abc.abstractmethod
    async def _subscribe(self) -> None:
        """Send subscription messages after connecting."""

    @abc.abstractmethod
    async def _listen(self) -> None:
        """Main loop: receive messages and call on_snapshot."""
