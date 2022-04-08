import asyncio
import unittest

from saq.job import Job, Status
from tests.helpers import create_queue, cleanup_queue


class TestJob(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.queue = create_queue()
        self.job = Job("func", queue=self.queue)

    async def asyncTearDown(self):
        await cleanup_queue(self.queue)

    def test_duration(self):
        self.assertIsNone(Job("").duration("process"))
        self.assertIsNone(Job("").duration("start"))
        self.assertIsNone(Job("").duration("total"))
        with self.assertRaises(ValueError):
            Job("").duration("x")

        self.assertEqual(Job("", completed=2, started=1).duration("process"), 1)
        self.assertEqual(Job("", started=2, queued=1).duration("start"), 1)
        self.assertEqual(Job("", completed=2, queued=1).duration("total"), 1)

    async def test_enqueue(self):
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
            await self.job.enqueue(create_queue(name="queue2"))

    async def test_finish(self):
        await self.job.finish(Status.COMPLETE, result={})
        self.assertEqual(self.job.status, Status.COMPLETE)
        self.assertEqual(self.job.result, {})

    async def test_retry(self):
        await self.job.retry("error")
        self.assertEqual(self.job.error, "error")

    async def test_update(self):
        self.assertEqual(self.job.attempts, 0)
        self.job.attempts += 1
        await self.job.update()
        self.assertEqual(self.job.attempts, 1)

    async def test_refresh(self):
        with self.assertRaises(RuntimeError):
            await self.job.refresh()

        await self.job.enqueue()
        await self.job.refresh()
        with self.assertRaises(asyncio.TimeoutError):
            await self.job.refresh(0.01)

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.job.refresh(0), 0.1)

        async def finish():
            await asyncio.sleep(0.01)
            await self.job.finish(Status.COMPLETE)

        self.assertEqual(self.job.status, Status.QUEUED)
        asyncio.create_task(finish())
        await self.job.refresh(0.1)
        self.assertEqual(self.job.status, Status.COMPLETE)

    async def test_retry_delay(self):
        job = Job("f")
        self.assertAlmostEqual(job.next_retry_delay(), 0)
        job = Job("f", retry_delay=1.0)
        self.assertAlmostEqual(job.next_retry_delay(), 1.0)
        job = Job("f", retry_delay=1.0, retry_backoff=True, attempts=3)
        self.assertTrue(0 <= job.next_retry_delay() < 4)

    async def test_meta(self):
        self.assertNotIn("meta", Job("f").to_dict())
        job = Job("f", meta={"a": 1})
        self.assertEqual(job.to_dict()["meta"], {"a": 1})
        job = Job("f")
        job.meta["a"] = 1
        self.assertEqual(job.to_dict()["meta"], {"a": 1})
