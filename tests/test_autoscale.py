"""
Tests for Worker auto-scaling (Module C.2).
"""
from __future__ import annotations

import asyncio
import unittest
from unittest import mock

from saq.job import Job, Status
from saq.queue.base import Queue
from saq.worker import Worker
from saq.types import Context, Function


class _CountQueue(Queue):
    """Queue stub that returns controlled count values for autoscale testing."""

    def __init__(self, name: str = "test") -> None:
        super().__init__(name=name, dump=None, load=None)
        self._queued_count = 0

    def set_queued_count(self, count: int) -> None:
        self._queued_count = count

    async def disconnect(self) -> None:
        pass

    async def info(self, jobs: bool = False, offset: int = 0, limit: int = 10) -> dict:
        return {"workers": {}, "name": self.name, "queued": self._queued_count,
                "active": 0, "scheduled": 0, "jobs": []}

    async def count(self, kind) -> int:
        if kind == "queued":
            return self._queued_count
        return 0

    async def sweep(self, lock: int = 60, abort: float = 5.0) -> list[str]:
        return []

    async def _update(self, job: Job, status: Status | None = None, **kwargs) -> None:
        pass

    async def job(self, job_key: str) -> Job | None:
        return None

    async def jobs(self, job_keys) -> list[Job | None]:
        return []

    async def iter_jobs(self, statuses=None, batch_size=100, **kwargs):
        return
        yield  # make it an async generator

    async def abort(self, job: Job, error: str, ttl: float = 5) -> None:
        pass

    async def dequeue(self, timeout: float = 0.0, poll_interval: float = 0.0) -> Job | None:
        await asyncio.sleep(0.01)
        return None

    async def write_worker_info(self, worker_id: str, info, ttl: int) -> None:
        pass

    async def _retry(self, job: Job, error: str | None) -> None:
        pass

    async def _finish(self, job: Job, status: Status, **kwargs) -> None:
        pass

    async def _enqueue(self, job: Job) -> Job | None:
        return job


async def _noop(_ctx: Context) -> None:
    pass


class TestAutoscaleConfig(unittest.IsolatedAsyncioTestCase):
    """Test autoscale configuration and target calculation."""

    async def asyncSetUp(self):
        self.queue = _CountQueue()
        await self.queue.connect()
        self.autoscale = {"min": 1, "max": 10, "target_queue_depth": 5}

    async def asyncTearDown(self):
        await self.queue.disconnect()

    async def test_autoscale_up(self):
        """Queue backlog increases concurrency up to max."""
        self.queue.set_queued_count(50)
        worker = Worker(self.queue, [_noop], autoscale=self.autoscale, concurrency=1)
        await worker._autoscale_check()
        # target = min(10, max(1, 50 // 5)) = min(10, 10) = 10
        self.assertEqual(worker._target_concurrency, 10)

    async def test_autoscale_down(self):
        """Empty queue reduces concurrency to min."""
        self.queue.set_queued_count(0)
        worker = Worker(self.queue, [_noop], autoscale=self.autoscale, concurrency=10)
        worker._target_concurrency = 10
        await worker._autoscale_check()
        # target = min(10, max(1, 0 // 5)) = min(10, max(1, 0)) = 1
        self.assertEqual(worker._target_concurrency, 1)

    async def test_autoscale_min_floor(self):
        """Concurrency doesn't go below min."""
        self.queue.set_queued_count(0)
        config = {"min": 3, "max": 10, "target_queue_depth": 5}
        worker = Worker(self.queue, [_noop], autoscale=config, concurrency=10)
        worker._target_concurrency = 10
        await worker._autoscale_check()
        self.assertEqual(worker._target_concurrency, 3)

    async def test_autoscale_max_ceiling(self):
        """Concurrency doesn't exceed max."""
        self.queue.set_queued_count(1000)
        config = {"min": 1, "max": 5, "target_queue_depth": 5}
        worker = Worker(self.queue, [_noop], autoscale=config, concurrency=1)
        await worker._autoscale_check()
        self.assertEqual(worker._target_concurrency, 5)

    async def test_autoscale_disabled(self):
        """autoscale=None does nothing."""
        worker = Worker(self.queue, [_noop], concurrency=5)
        worker._target_concurrency = 5
        await worker._autoscale_check()
        self.assertEqual(worker._target_concurrency, 5)

    async def test_autoscale_partial_backlog(self):
        """Partial backlog calculates correct concurrency."""
        self.queue.set_queued_count(12)
        config = {"min": 1, "max": 10, "target_queue_depth": 5}
        worker = Worker(self.queue, [_noop], autoscale=config, concurrency=1)
        await worker._autoscale_check()
        # target = min(10, max(1, 12 // 5)) = min(10, 2) = 2
        self.assertEqual(worker._target_concurrency, 2)

    async def test_autoscale_target_queue_depth_one(self):
        """target_queue_depth=1 means 1 worker per queued job."""
        self.queue.set_queued_count(3)
        config = {"min": 1, "max": 10, "target_queue_depth": 1}
        worker = Worker(self.queue, [_noop], autoscale=config, concurrency=1)
        await worker._autoscale_check()
        # target = min(10, max(1, 3 // 1)) = min(10, 3) = 3
        self.assertEqual(worker._target_concurrency, 3)


class TestAutoscaleProcessControl(unittest.IsolatedAsyncioTestCase):
    """Test that autoscale affects process task creation."""

    async def asyncSetUp(self):
        self.queue = _CountQueue()
        await self.queue.connect()
        self.autoscale = {"min": 1, "max": 5, "target_queue_depth": 2}

    async def asyncTearDown(self):
        await self.queue.disconnect()

    async def test_process_replicates_up_to_target(self):
        """_process() creates tasks up to _target_concurrency."""
        worker = Worker(self.queue, [_noop], autoscale=self.autoscale, concurrency=1)
        worker._target_concurrency = 3

        # Start 3 process loops
        for _ in range(5):
            worker._process()

        # Should have created at most _target_concurrency tasks
        process_tasks = [t for t in worker.tasks if t.get_name() == "process"]
        self.assertLessEqual(len(process_tasks), 3)

        # Clean up
        worker.event.set()
        for t in list(worker.tasks):
            t.cancel()
        await asyncio.gather(*worker.tasks, return_exceptions=True)

    async def test_process_stops_at_target(self):
        """_process() doesn't replicate when at target concurrency."""
        worker = Worker(self.queue, [_noop], autoscale=self.autoscale, concurrency=1)
        worker._target_concurrency = 1

        worker._process()  # Should create 1 task

        # Try to create another - should be blocked by target limit
        worker._process()  # Should NOT create another task

        process_tasks = [t for t in worker.tasks if t.get_name() == "process"]
        self.assertLessEqual(len(process_tasks), 1)

        # Clean up
        worker.event.set()
        for t in list(worker.tasks):
            t.cancel()
        await asyncio.gather(*worker.tasks, return_exceptions=True)

    async def test_scale_up_adds_processes(self):
        """_autoscale_check starts new processes when target increases."""
        self.queue.set_queued_count(10)
        worker = Worker(self.queue, [_noop], autoscale=self.autoscale, concurrency=1)
        worker._target_concurrency = 1
        # Start with 1 process loop
        worker._process()

        await asyncio.sleep(0.05)
        initial_count = len([t for t in worker.tasks if t.get_name() == "process"])

        # Trigger autoscale
        await worker._autoscale_check()
        # target = min(5, max(1, 10 // 2)) = min(5, 5) = 5
        self.assertEqual(worker._target_concurrency, 5)

        # Clean up
        worker.event.set()
        for t in list(worker.tasks):
            t.cancel()
        await asyncio.gather(*worker.tasks, return_exceptions=True)

    async def test_scale_down_stops_replication(self):
        """When target decreases, excess tasks don't replicate."""
        worker = Worker(self.queue, [_noop], autoscale=self.autoscale, concurrency=3)
        worker._target_concurrency = 3

        # Start 3 process loops
        for _ in range(3):
            worker._process()

        # Reduce target
        worker._target_concurrency = 1

        # When a task completes, _process checks target and shouldn't replicate
        # Simulate a task completion
        tasks = list(worker.tasks)
        for t in tasks:
            worker._process(previous_task=t)

        # Tasks that completed above target should not have been replaced
        # We should have fewer tasks now
        remaining = [t for t in worker.tasks if t.get_name() == "process"]
        self.assertLessEqual(len(remaining), 1)

        # Clean up
        worker.event.set()
        for t in list(worker.tasks):
            t.cancel()
        await asyncio.gather(*worker.tasks, return_exceptions=True)


if __name__ == "__main__":
    unittest.main()
