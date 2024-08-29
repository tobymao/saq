from __future__ import annotations

import asyncio
import typing as t
import unittest
from unittest import mock

from saq.job import Job, Status
from tests.helpers import (
    cleanup_queue,
    create_postgres_queue,
    create_redis_queue,
    setup_postgres,
    teardown_postgres,
)


if t.TYPE_CHECKING:
    from unittest.mock import MagicMock

    from saq.queue import Queue


class TestJob(unittest.IsolatedAsyncioTestCase):
    queue: Queue
    job: Job
    create_queue: t.Callable

    async def asyncSetUp(self) -> None:
        self.skipTest("Skipping base test case")

    async def asyncTearDown(self) -> None:
        await cleanup_queue(self.queue)

    def test_duration(self) -> None:
        self.assertIsNone(Job("").duration("process"))
        self.assertIsNone(Job("").duration("start"))
        self.assertIsNone(Job("").duration("total"))
        with self.assertRaises(ValueError):
            Job("").duration("x")  # type:ignore[arg-type]

        self.assertEqual(Job("", completed=2, started=1).duration("process"), 1)
        self.assertEqual(Job("", started=2, queued=1).duration("start"), 1)
        self.assertEqual(Job("", completed=2, queued=1).duration("total"), 1)

    async def test_enqueue(self) -> None:
        self.assertEqual(await self.queue.count("queued"), 0)
        self.assertEqual(self.job.status, Status.NEW)
        await self.job.enqueue()
        self.assertEqual(self.job.status, Status.QUEUED)
        self.assertIsNotNone(self.job.queued)
        self.assertEqual(self.job.started, 0)
        self.assertEqual(await self.queue.count("queued"), 1)

        queued = self.job.queued
        await self.job.enqueue()
        self.assertEqual(queued, self.job.queued)

        with self.assertRaises(ValueError):
            await self.job.enqueue(await self.create_queue(name="queue2"))

    async def test_finish(self) -> None:
        await self.job.finish(Status.COMPLETE, result={})
        self.assertEqual(self.job.status, Status.COMPLETE)
        self.assertEqual(self.job.result, {})

    async def test_retry(self) -> None:
        await self.job.retry("error")
        self.assertEqual(self.job.error, "error")

    async def test_stuck(self) -> None:
        self.assertFalse(Job("", status=Status.ACTIVE, timeout=0).stuck)

    async def test_update(self) -> None:
        self.assertEqual(self.job.attempts, 0)
        self.job.attempts += 1
        await self.job.update()
        self.assertEqual(self.job.attempts, 1)

    async def test_refresh(self) -> None:
        with self.assertRaises(RuntimeError):
            await self.job.refresh()

        await self.job.enqueue()
        await self.job.refresh()
        with self.assertRaises(asyncio.TimeoutError):
            await self.job.refresh(0.01)

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.job.refresh(0), 0.1)

        self.assertEqual(self.job.status, Status.QUEUED)
        task = asyncio.create_task(self.job.refresh())
        await self.job.finish(Status.COMPLETE)
        await task
        self.assertEqual(self.job.status, Status.COMPLETE)

    async def test_retry_delay(self) -> None:
        job = Job("f")
        self.assertAlmostEqual(job.next_retry_delay(), 0)
        job = Job("f", retry_delay=1.0)
        self.assertAlmostEqual(job.next_retry_delay(), 1.0)
        job = Job("f", retry_delay=1.0, retry_backoff=True, attempts=3)
        self.assertTrue(0 <= job.next_retry_delay() < 4)

    @mock.patch("saq.job.exponential_backoff")
    async def test_next_retry_delay_with_max_delay(self, eb_mock: MagicMock) -> None:
        job = Job("f", retry_delay=1.0, retry_backoff=10, attempts=3)
        job.next_retry_delay()
        eb_mock.assert_called_once_with(
            attempts=3, base_delay=1.0, max_delay=10, jitter=True
        )

    @mock.patch("saq.job.exponential_backoff")
    async def test_next_retry_delay_no_maximum(self, eb_mock: MagicMock) -> None:
        job = Job("f", retry_delay=1.0, retry_backoff=True, attempts=3)
        job.next_retry_delay()
        eb_mock.assert_called_once_with(
            attempts=3, base_delay=1.0, max_delay=None, jitter=True
        )

    async def test_to_dict(self) -> None:
        assert Job("f", key="a").to_dict() == {"function": "f", "key": "a"}
        assert Job("f", key="a", meta={"x": 1}, queue=self.queue).to_dict() == {
            "function": "f",
            "key": "a",
            "queue": self.queue.name,
            "meta": {"x": 1},
        }


class TestJobRedisQueue(TestJob):
    async def asyncSetUp(self) -> None:
        self.create_queue = create_redis_queue
        self.queue = await self.create_queue()
        self.job = Job("func", queue=self.queue)


class TestJobPostgresQueue(TestJob):
    async def asyncSetUp(self) -> None:
        await setup_postgres()
        self.create_queue = create_postgres_queue
        self.queue = await self.create_queue()
        self.job = Job("func", queue=self.queue)

    async def asyncTearDown(self) -> None:
        await super().asyncTearDown()
        await teardown_postgres()

    async def test_refresh(self) -> None:
        with self.assertRaises(RuntimeError):
            await self.job.refresh()

        await self.job.enqueue()
        await self.job.refresh()
        with self.assertRaises(asyncio.TimeoutError):
            await self.job.refresh(0.01)

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.job.refresh(0), 0.1)

        self.assertEqual(self.job.status, Status.QUEUED)
        task = asyncio.create_task(self.job.refresh(1))
        await asyncio.sleep(0)
        await self.job.finish(Status.COMPLETE)
        await task
        self.assertEqual(self.job.status, Status.COMPLETE)
