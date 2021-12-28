import asyncio
import unittest

from saq.job import Job, Status
from saq.queue import Queue


class TestJob(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.queue = Queue.from_url("redis::localhost:6379")
        self.job = Job("func", queue=self.queue)

    async def asyncTearDown(self):
        await self.queue.redis.flushdb()
        await self.queue.redis.close()

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

        with self.assertRaises(ValueError):
            await self.job.enqueue(
                Queue.from_url("redis://localhost:6379", name="queue2")
            )

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
            await self.job.refresh(0.01, 0.01)

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(self.job.refresh(0), 0.1)

        async def finish():
            await asyncio.sleep(0.01)
            await self.job.finish(Status.COMPLETE)

        self.assertEqual(self.job.status, Status.QUEUED)
        asyncio.create_task(finish())
        await self.job.refresh(0.05, 0.01)
        self.assertEqual(self.job.status, Status.COMPLETE)
