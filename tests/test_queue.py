import asyncio
import time
import unittest
from unittest import mock

from saq.job import Job, Status
from saq.queue import JobError
from saq.utils import uuid1
from saq.worker import Worker
from tests.helpers import create_queue, cleanup_queue


async def echo(_ctx, *, a):
    return a


async def error(_ctx):
    raise ValueError("oops")


functions = [echo, error]


class TestQueue(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.queue = create_queue()

    async def asyncTearDown(self):
        await cleanup_queue(self.queue)

    async def test_enqueue_job(self):
        job = Job("test")
        self.assertEqual(await self.queue.enqueue(job), await self.queue.job(job.key))
        self.assertEqual(await self.queue.count("queued"), 1)
        await self.queue.enqueue(job)
        self.assertEqual(await self.queue.count("queued"), 1)
        await self.queue.enqueue(Job("test"))
        self.assertEqual(await self.queue.count("queued"), 2)

    async def test_enqueue_job_str(self):
        job = await self.queue.enqueue("test")
        self.assertIsNotNone(job)
        self.assertEqual(await self.queue.job(job.key), job)
        job = await self.queue.enqueue("test", y=1, timeout=1)
        self.assertEqual(job.kwargs, {"y": 1})
        self.assertEqual(job.timeout, 1)
        self.assertEqual(job.heartbeat, 0)

    async def test_enqueue_dup(self):
        job = await self.queue.enqueue("test", key="1")
        self.assertEqual(job.id, "saq:job:default:1")
        self.assertIsNone(await self.queue.enqueue("test", key="1"))
        self.assertIsNone(await self.queue.enqueue(job))

    async def test_enqueue_scheduled(self):
        scheduled = time.time() + 10
        job = await self.queue.enqueue("test", scheduled=scheduled)
        self.assertEqual(await self.queue.count("queued"), 0)
        self.assertEqual(await self.queue.count("incomplete"), 1)
        self.assertEqual(
            await self.queue.redis.zscore(self.queue.namespace("incomplete"), job.id),
            scheduled,
        )

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

    async def test_dequeue_timeout(self):
        dequeued = await self.queue.dequeue(timeout=0.1)
        self.assertEqual(None, dequeued)
        self.assertEqual(await self.queue.count("queued"), 0)
        self.assertEqual(await self.queue.count("incomplete"), 0)
        self.assertEqual(await self.queue.count("active"), 0)

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

    async def test_retry_delay(self):
        # Let's first verify how things work without a retry delay
        worker = Worker(self.queue, functions=functions, dequeue_timeout=0.01)
        job = await self.queue.enqueue("error", retries=2)
        await worker.process()
        await job.refresh()
        self.assertEqual(job.status, Status.QUEUED)
        await worker.process()
        await job.refresh()
        self.assertEqual(job.status, Status.FAILED)

        # Now with the delay
        job = await self.queue.enqueue("error", retries=2, retry_delay=100.0)
        await worker.process()
        await job.refresh()
        self.assertEqual(job.status, Status.QUEUED)
        await worker.process()
        await job.refresh()
        self.assertEqual(job.status, Status.QUEUED)

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
        self.assertEqual(await self.queue.redis.get(job.abort_id), b"test")

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
        self.assertGreater(stats["uptime"], 0)

    async def test_info(self):
        queue2 = create_queue(name=self.queue.name)
        self.addAsyncCleanup(cleanup_queue, queue2)
        worker = Worker(self.queue, functions=functions)
        info = await self.queue.info(jobs=True)
        self.assertEqual(info["workers"], {})
        self.assertEqual(info["active"], 0)
        self.assertEqual(info["queued"], 0)
        self.assertEqual(info["scheduled"], 0)
        self.assertEqual(info["jobs"], [])

        await self.queue.enqueue("echo", a=1)
        await queue2.enqueue("echo", a=1)
        await worker.process()
        await self.queue.stats()
        await queue2.stats()

        info = await self.queue.info(jobs=True)
        self.assertEqual(set(info["workers"].keys()), {self.queue.uuid, queue2.uuid})
        self.assertEqual(info["active"], 0)
        self.assertEqual(info["queued"], 1)
        self.assertEqual(len(info["jobs"]), 1)

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

        # missing job
        job4 = Job(function="", queue=self.queue)

        # pylint: disable=protected-access
        await self.queue.redis.lpush(self.queue._active, job4.id)

        mock_time.time.return_value = 3
        self.assertEqual(await self.queue.count("active"), 6)
        swept = await self.queue.sweep()
        self.assertEqual(
            set(swept),
            {
                job1.id.encode(),
                job2.id.encode(),
                job3.id.encode(),
                job4.id.encode(),
            },
        )
        await job1.refresh()
        await job2.refresh()
        await job3.refresh()
        self.assertEqual(job1.status, Status.ABORTED)
        self.assertEqual(job2.status, Status.ABORTED)
        self.assertEqual(job3.status, Status.ABORTED)
        self.assertEqual(await self.queue.count("active"), 2)

    async def test_update(self):
        job = await self.queue.enqueue("test")
        counter = {"x": 0}

        def listen(job_key, status):
            self.assertEqual(job.key, job_key)
            self.assertEqual(status, Status.QUEUED)
            counter["x"] += 1
            return counter["x"] == 2

        task = asyncio.create_task(self.queue.listen([job.key], listen, timeout=0.1))
        await asyncio.sleep(0)
        await self.queue.update(job)
        await self.queue.update(job)
        await task
        self.assertEqual(counter["x"], 2)

    async def test_apply(self):
        worker = Worker(self.queue, functions=functions)
        task = asyncio.create_task(worker.start())

        self.assertEqual(await self.queue.apply("echo", a=1), 1)
        with self.assertRaises(JobError):
            await self.queue.apply("error")

        task.cancel()

    async def test_map(self):
        worker = Worker(self.queue, functions=functions)
        task = asyncio.create_task(worker.start())

        self.assertEqual(await self.queue.map("echo", []), [])
        self.assertEqual(await self.queue.map("echo", [{"a": 1}]), [1])
        self.assertEqual(await self.queue.map("echo", [{"a": 1}, {"a": 2}]), [1, 2])
        self.assertEqual(
            await self.queue.map("echo", [{}, {"a": 2}], timeout=10, a=3), [3, 2]
        )
        with self.assertRaises(JobError):
            await self.queue.map("error", [{}, {}])

        results = await self.queue.map("error", [{}, {}], return_exceptions=True)
        self.assertTrue(all(isinstance(r, JobError) for r in results))

        key = uuid1()
        await self.queue.enqueue("echo", key=key, a=3)
        self.assertEqual(await self.queue.map("echo", [{"a": 1, "key": key}]), [3])

        task.cancel()

    async def test_batch(self):
        job = None
        try:
            async with self.queue.batch():
                job = await self.queue.enqueue("echo", a=1)
                raise ValueError()
        except ValueError:
            pass

        self.assertEqual(job.status, Status.ABORTED)

    async def test_before_enqueue(self):
        called_with_job = None

        async def callback(job):
            nonlocal called_with_job
            called_with_job = job

        self.queue.register_before_enqueue(callback)
        await self.queue.enqueue("test")
        self.assertIsNotNone(called_with_job)

        called_with_job = None
        self.queue.unregister_before_enqueue(callback)
        await self.queue.enqueue("test")
        self.assertIsNone(called_with_job)
