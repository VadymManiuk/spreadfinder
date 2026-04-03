"""
Tests for BaseExchangeAdapter task coordination helpers.

Inputs: Synthetic async tasks and a no-op adapter implementation.
Outputs: Assertions over task cancellation and exception collection.
Assumptions:
  - The helper should consume task results to avoid unhandled task exceptions.
"""

import asyncio

import pytest

from exchange_adapters.base import BaseExchangeAdapter


class DummyAdapter(BaseExchangeAdapter):

    def __init__(self) -> None:
        async def on_snapshot(_snapshot) -> None:
            return None

        super().__init__(exchange_name="dummy", on_snapshot=on_snapshot)

    async def _connect(self) -> None:
        return None

    async def _disconnect(self) -> None:
        return None

    async def _subscribe(self) -> None:
        return None

    async def _listen(self) -> None:
        return None


class TestBaseExchangeAdapter:

    @pytest.mark.asyncio
    async def test_wait_until_first_task_finishes_collects_parallel_failures(self):
        adapter = DummyAdapter()

        async def fail(message: str) -> None:
            await asyncio.sleep(0)
            raise RuntimeError(message)

        tasks = [
            asyncio.create_task(fail("first")),
            asyncio.create_task(fail("second")),
        ]

        error = await adapter._wait_until_first_task_finishes(tasks)

        assert isinstance(error, RuntimeError)
        assert str(error) in {"first", "second"}
        assert all(task.done() for task in tasks)

    @pytest.mark.asyncio
    async def test_cancel_tasks_cancels_pending_work(self):
        adapter = DummyAdapter()
        started = asyncio.Event()

        async def wait_forever() -> None:
            started.set()
            await asyncio.sleep(60)

        task = asyncio.create_task(wait_forever())
        await started.wait()

        await adapter._cancel_tasks([task])

        assert task.cancelled()
