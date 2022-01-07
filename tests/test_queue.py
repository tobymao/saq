import asyncio
import time
import unittest
from unittest import mock

from saq.job import Job, Status
from saq.queue import Queue
from tests.helpers import create_queue, cleanup_queue


class TestQueue(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.queue = create_queue()

    async def asyncTearDown(self):
        await cleanup_queue(self.queue)

    async def test_enqueue_job(self):
        job = Job("test")
        self.assertEqual(await self.queue.enqueue(job), await self.queue.job(job.id))
        self.assertEqual(await self.queue.count("queued"), 1)
        await self.queue.enqueue(job)
        self.assertEqual(await self.queue.count("queued"), 1)
        await self.queue.enqueue(Job("test"))
        self.assertEqual(await self.queue.count("queued"), 2)

    async def test_enqueue_job_str(self):
        job = await self.queue.enqueue("test")
        self.assertIsNotNone(job)
        self.assertEqual(await self.queue.job(job.id), job)
        job = await self.queue.enqueue("test", y=1, timeout=1)
        self.assertEqual(job.kwargs, {"y": 1})
        self.assertEqual(job.timeout, 1)
        self.assertEqual(job.heartbeat, 0)

    async def test_enqueue_dup(self):
        job = await self.queue.enqueue("test", key="1")
        self.assertEqual(job.id, "saq:job:1")
        self.assertEqual(job, await self.queue.enqueue("test", key="1"))
        self.assertEqual(job, await self.queue.enqueue(job))

    async def test_enqueue_scheduled(self):
        scheduled = time.time() + 10
        job = await self.queue.enqueue("test", scheduled=scheduled)
        self.assertEqual(await self.queue.count("queued"), 0)
        self.assertEqual(await self.queue.count("incomplete"), 1)
        self.assertEqual(
            await self.queue.redis.zscore(self.queue.namespace("incomplete"), job.id),
            scheduled,
        )

    async def test_enqueue_job_classmethod(self):
        job = Job("test")
        await Queue.enqueue_job(job, self.queue.redis, queue_name="foo")
        self.assertEqual(job.queue.name, "foo")
        self.assertEqual(await job.queue.count("queued"), 1)

    async def test_dequeue(self):
        job = await self.queue.enqueue("test")
        self.assertEqual(await self.queue.count("queued"), 1)
        self.assertEqual(await self.queue.count("incomplete"), 1)
        self.assertEqual(await self.queue.count("active"), 0)
        dequeued = await self.queue.dequeue()
        self.assertEqual(job, dequeued)
        self.assertEqual(await self.queue.count("queued"), 0)
        self.assertEqual(await self.queue.count("incomplete"), 1)
        self.assertEqual(await self.queue.count("active"), 1)

        task = asyncio.get_running_loop().create_task(self.queue.dequeue())
        job = await self.queue.enqueue("test")
        await asyncio.sleep(0.1)
        self.assertEqual(await self.queue.count("queued"), 0)
        await task

    async def test_finish(self):
        job = await self.queue.enqueue("test")
        await self.queue.dequeue()
        self.assertEqual(await self.queue.count("queued"), 0)
        self.assertEqual(await self.queue.count("incomplete"), 1)
        self.assertEqual(await self.queue.count("active"), 1)
        await self.queue.finish(job, Status.COMPLETE, result=1)
        self.assertEqual(job.status, Status.COMPLETE)
        self.assertEqual(job.result, 1)
        self.assertEqual(await self.queue.count("queued"), 0)
        self.assertEqual(await self.queue.count("incomplete"), 0)
        self.assertEqual(await self.queue.count("active"), 0)

    async def test_retry(self):
        job = await self.queue.enqueue("test", retries=2)
        await self.queue.dequeue()
        self.assertEqual(await self.queue.count("queued"), 0)
        self.assertEqual(await self.queue.count("incomplete"), 1)
        self.assertEqual(await self.queue.count("active"), 1)
        self.assertEqual(self.queue.retried, 0)
        await self.queue.retry(job, None)
        self.assertEqual(self.queue.retried, 1)
        self.assertEqual(await self.queue.count("queued"), 1)
        self.assertEqual(await self.queue.count("incomplete"), 1)
        self.assertEqual(await self.queue.count("active"), 0)

    async def test_abort(self):
        job = await self.queue.enqueue("test", retries=2)
        self.assertEqual(await self.queue.count("queued"), 1)
        self.assertEqual(await self.queue.count("incomplete"), 1)
        await self.queue.abort(job, "test")
        self.assertEqual(await self.queue.count("queued"), 0)
        self.assertEqual(await self.queue.count("incomplete"), 0)

        job = await self.queue.enqueue("test", retries=2)
        await self.queue.dequeue()
        self.assertEqual(await self.queue.count("queued"), 0)
        self.assertEqual(await self.queue.count("incomplete"), 1)
        self.assertEqual(await self.queue.count("active"), 1)
        await self.queue.abort(job, "test")
        self.assertEqual(await self.queue.count("queued"), 0)
        self.assertEqual(await self.queue.count("incomplete"), 0)
        self.assertEqual(await self.queue.count("active"), 0)

    async def test_stats(self):
        for _ in range(10):
            await self.queue.enqueue("test")
            job = await self.queue.dequeue()
            await job.retry(None)
            await job.abort("test")
            await job.finish(Status.FAILED)
            await job.finish(Status.COMPLETE)
        stats = await self.queue.stats()
        self.assertEqual(stats["complete"], 10)
        self.assertEqual(stats["failed"], 10)
        self.assertEqual(stats["retried"], 10)
        self.assertEqual(stats["aborted"], 10)
        assert stats["uptime"] > 0

    @mock.patch("saq.utils.time")
    async def test_schedule(self, mock_time):
        mock_time.time.return_value = 2
        self.assertEqual(await self.queue.count("queued"), 0)
        self.assertEqual(await self.queue.count("incomplete"), 0)
        await self.queue.enqueue("test")
        job1 = await self.queue.enqueue("test", scheduled=1)
        job2 = await self.queue.enqueue("test", scheduled=2)
        await self.queue.enqueue("test", scheduled=3)
        self.assertEqual(await self.queue.count("queued"), 1)
        self.assertEqual(await self.queue.count("incomplete"), 4)
        jobs1 = await self.queue.schedule()
        jobs2 = await self.queue.schedule()
        self.assertEqual(jobs1, [job1.id.encode(), job2.id.encode()])
        self.assertIsNone(jobs2)
        self.assertEqual(await self.queue.count("queued"), 3)
        self.assertEqual(await self.queue.count("incomplete"), 4)

    @mock.patch("saq.utils.time")
    async def test_sweep(self, mock_time):
        mock_time.time.return_value = 1
        job1 = await self.queue.enqueue("test", heartbeat=1)
        job2 = await self.queue.enqueue("test", timeout=1)
        await self.queue.enqueue("test", timeout=2)
        await self.queue.enqueue("test", heartbeat=2)
        job3 = await self.queue.enqueue("test", timeout=1)
        for _ in range(4):
            job = await self.queue.dequeue()
            job.status = Status.ACTIVE
            job.started = 1000
            await self.queue.update(job)
        await self.queue.dequeue()
        mock_time.time.return_value = 3
        self.assertEqual(await self.queue.count("active"), 5)
        swept = await self.queue.sweep()
        self.assertEqual(
            swept,
            [
                job1.id.encode(),
                job2.id.encode(),
                job3.id.encode(),
            ],
        )
        job1 = await self.queue.job(job1.id)
        job2 = await self.queue.job(job2.id)
        job3 = await self.queue.job(job3.id)
        self.assertEqual(job1.status, Status.ABORTED)
        self.assertEqual(job2.status, Status.ABORTED)
        self.assertEqual(job3.status, Status.ABORTED)
        self.assertEqual(await self.queue.count("active"), 2)

    async def test_update(self):
        job = await self.queue.enqueue("test")
        counter = {"x": 0}

        def listen(job_id, status):
            self.assertEqual(job.id, job_id)
            self.assertEqual(status, Status.QUEUED)
            counter["x"] += 1
            return counter["x"] == 2

        task = asyncio.create_task(self.queue.listen(job, listen, 0.1))
        await asyncio.sleep(0)
        await self.queue.update(job)
        await self.queue.update(job)
        await task
        self.assertEqual(counter["x"], 2)
