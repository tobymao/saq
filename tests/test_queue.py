from __future__ import annotations

import asyncio
import contextlib
import time
import typing as t
import unittest
from unittest import mock

from saq.job import Job, Status
from saq.queue import JobError, Queue
from saq.queue.postgres import PostgresQueue
from saq.queue.redis import RedisQueue
from saq.utils import uuid1
from saq.worker import Worker
from tests.helpers import cleanup_queue, create_postgres_queue, create_queue

if t.TYPE_CHECKING:
    from unittest.mock import MagicMock

    from saq.types import Context, CountKind, Function


async def echo(_ctx: Context, *, a: int) -> int:
    return a


async def error(_ctx: Context) -> None:
    raise ValueError("oops")


functions: list[Function] = [echo, error]


class TestQueue(unittest.IsolatedAsyncioTestCase):
    queue: Queue

    async def asyncSetUp(self) -> None:
        self.skipTest("Skipping base test case")

    async def asyncTearDown(self) -> None:
        await cleanup_queue(self.queue)

    async def enqueue(self, job: Job | str, **kwargs: t.Any) -> Job:
        enqueued = await self.queue.enqueue(job, **kwargs)
        assert enqueued is not None
        return enqueued

    async def dequeue(self, **kwargs: t.Any) -> Job:
        dequeued = await self.queue.dequeue(**kwargs)
        assert dequeued is not None
        return dequeued

    async def count(self, kind: CountKind) -> int:
        return await self.queue.count(kind)

    async def finish(self, job: Job, status: Status, **kwargs: t.Any) -> None:
        await self.queue.finish(job, status, **kwargs)

    async def test_enqueue_job(self) -> None:
        job = Job("test")
        self.assertEqual(await self.enqueue(job), await self.queue.job(job.key))
        self.assertEqual(await self.count("queued"), 1)
        await self.queue.enqueue(job)
        self.assertEqual(await self.count("queued"), 1)
        await self.enqueue(Job("test"))
        self.assertEqual(await self.count("queued"), 2)

    async def test_enqueue_job_str(self) -> None:
        job = await self.enqueue("test")
        self.assertIsNotNone(job)
        self.assertEqual(await self.queue.job(job.key), job)
        job = await self.enqueue("test", y=1, timeout=1)
        self.assertEqual(job.kwargs, {"y": 1})
        self.assertEqual(job.timeout, 1)
        self.assertEqual(job.heartbeat, 0)

    async def test_enqueue_dup(self) -> None:
        job = await self.enqueue("test", key="1")
        self.assertEqual(job.id, "saq:job:default:1")
        self.assertIsNone(await self.queue.enqueue("test", key="1"))
        self.assertIsNone(await self.queue.enqueue(job))

    async def test_dequeue(self) -> None:
        job = await self.enqueue("test")
        self.assertEqual(await self.count("queued"), 1)
        self.assertEqual(await self.count("incomplete"), 1)
        self.assertEqual(await self.count("active"), 0)
        dequeued = await self.dequeue()
        self.assertEqual(job, dequeued)
        self.assertEqual(await self.count("queued"), 0)
        self.assertEqual(await self.count("incomplete"), 1)
        self.assertEqual(await self.count("active"), 1)

        task = asyncio.get_running_loop().create_task(self.dequeue())
        await self.enqueue("test")
        await asyncio.sleep(0.1)
        self.assertEqual(await self.count("queued"), 0)
        await task

    async def test_dequeue_fifo(self) -> None:
        await cleanup_queue(
            self.queue  # pylint: disable=access-member-before-definition
        )
        self.queue = create_queue()
        job = await self.enqueue("test")
        job_second = await self.enqueue("test_second")
        self.assertEqual(await self.count("queued"), 2)
        self.assertEqual(await self.count("incomplete"), 2)
        self.assertEqual(await self.count("active"), 0)
        dequeued = await self.dequeue()
        self.assertEqual(job, dequeued)
        self.assertEqual(await self.count("queued"), 1)
        self.assertEqual(await self.count("incomplete"), 2)
        self.assertEqual(await self.count("active"), 1)

        await self.enqueue("test")
        self.assertEqual(await self.count("queued"), 2)
        dequeued = await self.dequeue()
        self.assertEqual(job_second, dequeued)
        self.assertEqual(await self.count("queued"), 1)
        self.assertEqual(await self.count("incomplete"), 3)
        self.assertEqual(await self.count("active"), 2)
        task = asyncio.get_running_loop().create_task(self.dequeue())
        self.assertEqual(await self.count("queued"), 1)
        self.assertEqual(await self.count("incomplete"), 3)
        await asyncio.sleep(0.05)
        self.assertEqual(await self.count("active"), 3)
        await task
        self.assertEqual(await self.count("incomplete"), 3)
        self.assertEqual(await self.count("queued"), 0)

    async def test_dequeue_timeout(self) -> None:
        dequeued = await self.queue.dequeue(timeout=1)
        self.assertEqual(None, dequeued)
        self.assertEqual(await self.count("queued"), 0)
        self.assertEqual(await self.count("incomplete"), 0)
        self.assertEqual(await self.count("active"), 0)

    async def test_finish(self) -> None:
        job = await self.enqueue("test")
        await self.dequeue()
        self.assertEqual(await self.count("queued"), 0)
        self.assertEqual(await self.count("incomplete"), 1)
        self.assertEqual(await self.count("active"), 1)
        await self.finish(job, Status.COMPLETE, result=1)
        self.assertEqual(job.status, Status.COMPLETE)
        self.assertEqual(job.result, 1)
        self.assertEqual(await self.count("queued"), 0)
        self.assertEqual(await self.count("incomplete"), 0)
        self.assertEqual(await self.count("active"), 0)

    async def test_retry(self) -> None:
        job = await self.enqueue("test", retries=2)
        await self.dequeue()
        self.assertEqual(await self.count("queued"), 0)
        self.assertEqual(await self.count("incomplete"), 1)
        self.assertEqual(await self.count("active"), 1)
        self.assertEqual(self.queue.retried, 0)
        await self.queue.retry(job, None)
        self.assertEqual(self.queue.retried, 1)
        self.assertEqual(await self.count("queued"), 1)
        self.assertEqual(await self.count("incomplete"), 1)
        self.assertEqual(await self.count("active"), 0)

    async def test_retry_delay(self) -> None:
        # Let's first verify how things work without a retry delay
        worker = Worker(self.queue, functions=functions, dequeue_timeout=0.01)
        job = await self.enqueue("error", retries=2)
        await worker.process()
        await job.refresh()
        self.assertEqual(job.status, Status.QUEUED)
        await worker.process()
        await job.refresh()
        self.assertEqual(job.status, Status.FAILED)

        # Now with the delay
        job = await self.enqueue("error", retries=2, retry_delay=100.0)
        await worker.process()
        await job.refresh()
        self.assertEqual(job.status, Status.QUEUED)
        await worker.process()
        await job.refresh()
        self.assertEqual(job.status, Status.QUEUED)

    async def test_stats(self) -> None:
        for _ in range(10):
            await self.enqueue("test")
            job = await self.dequeue()
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

    async def test_info(self) -> None:
        queue2 = create_queue(name=self.queue.name)
        self.addAsyncCleanup(cleanup_queue, queue2)
        worker = Worker(self.queue, functions=functions)
        info = await self.queue.info(jobs=True)
        self.assertEqual(info["workers"], {})
        self.assertEqual(info["active"], 0)
        self.assertEqual(info["queued"], 0)
        self.assertEqual(info["scheduled"], 0)
        self.assertEqual(info["jobs"], [])

        await self.enqueue("echo", a=1)
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
    async def test_schedule(self, mock_time: MagicMock) -> None:
        mock_time.time.return_value = 2
        self.assertEqual(await self.count("queued"), 0)
        self.assertEqual(await self.count("incomplete"), 0)
        await self.enqueue("test")
        job1 = await self.enqueue("test", scheduled=1)
        job2 = await self.enqueue("test", scheduled=2)
        await self.enqueue("test", scheduled=3)
        self.assertEqual(await self.count("queued"), 1)
        self.assertEqual(await self.count("incomplete"), 4)
        jobs1 = await self.queue.schedule()
        jobs2 = await self.queue.schedule()
        self.assertEqual(jobs1, [job1.id.encode(), job2.id.encode()])
        self.assertIsNone(jobs2)
        self.assertEqual(await self.count("queued"), 3)
        self.assertEqual(await self.count("incomplete"), 4)

    async def test_update(self) -> None:
        job = await self.enqueue("test")
        counter = {"x": 0}

        def listen(job_key: str, status: Status) -> bool:
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

    async def test_apply(self) -> None:
        worker = Worker(self.queue, functions=functions)
        task = asyncio.create_task(worker.start())

        self.assertEqual(await self.queue.apply("echo", a=1, ttl=-1), None)

        self.assertEqual(await self.queue.apply("echo", a=1), 1)
        with self.assertRaises(JobError):
            await self.queue.apply("error")

        task.cancel()

    async def test_map(self) -> None:
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
        await self.enqueue("echo", key=key, a=3)
        self.assertEqual(await self.queue.map("echo", [{"a": 1, "key": key}]), [3])

        task.cancel()

    async def test_batch(self) -> None:
        with contextlib.suppress(ValueError):
            async with self.queue.batch():
                job = await self.enqueue("echo", a=1)
                raise ValueError()

        self.assertEqual(job.status, Status.ABORTED)

    async def test_before_enqueue(self) -> None:
        called_with_job = None

        async def callback(job: Job) -> None:
            nonlocal called_with_job
            called_with_job = job

        self.queue.register_before_enqueue(callback)
        await self.enqueue("test")
        self.assertIsNotNone(called_with_job)

        called_with_job = None
        self.queue.unregister_before_enqueue(callback)
        await self.enqueue("test")
        self.assertIsNone(called_with_job)


class TestRedisQueue(TestQueue):
    async def asyncSetUp(self) -> None:
        self.queue: RedisQueue = create_queue()

    async def test_enqueue_scheduled(self) -> None:
        scheduled = time.time() + 10
        job = await self.enqueue("test", scheduled=scheduled)
        self.assertEqual(await self.count("queued"), 0)
        self.assertEqual(await self.count("incomplete"), 1)
        self.assertEqual(
            await self.queue.redis.zscore(self.queue.namespace("incomplete"), job.id),
            scheduled,
        )

    async def test_finish_ttl_positive(self) -> None:
        job = await self.enqueue("test", ttl=5)
        await self.dequeue()
        await self.finish(job, Status.COMPLETE)
        ttl = await self.queue.redis.ttl(job.id)

        self.assertLessEqual(ttl, 5)

    async def test_finish_ttl_neutral(self) -> None:
        job = await self.enqueue("test", ttl=0)
        await self.dequeue()
        await self.finish(job, Status.COMPLETE)
        ttl = await self.queue.redis.ttl(job.id)

        self.assertEqual(ttl, -1)

    async def test_finish_ttl_negative(self) -> None:
        job = await self.enqueue("test", ttl=-1)
        await self.dequeue()
        await self.finish(job, Status.COMPLETE)
        ttl = await self.queue.redis.ttl(job.id)

        self.assertEqual(ttl, -2)

    async def test_abort(self) -> None:
        job = await self.enqueue("test", retries=2)
        self.assertEqual(await self.count("queued"), 1)
        self.assertEqual(await self.count("incomplete"), 1)
        await self.queue.abort(job, "test")
        self.assertEqual(await self.count("queued"), 0)
        self.assertEqual(await self.count("incomplete"), 0)

        job = await self.enqueue("test", retries=2)
        await self.dequeue()
        self.assertEqual(await self.count("queued"), 0)
        self.assertEqual(await self.count("incomplete"), 1)
        self.assertEqual(await self.count("active"), 1)
        await self.queue.abort(job, "test")
        self.assertEqual(await self.count("queued"), 0)
        self.assertEqual(await self.count("incomplete"), 0)
        self.assertEqual(await self.count("active"), 0)
        self.assertEqual(await self.queue.redis.get(job.abort_id), b"test")

    @mock.patch("saq.utils.time")
    async def test_sweep(self, mock_time: MagicMock) -> None:
        mock_time.time.return_value = 1
        job1 = await self.enqueue("test", heartbeat=1, retries=0)
        job2 = await self.enqueue("test", timeout=1)
        await self.enqueue("test", timeout=2)
        await self.enqueue("test", heartbeat=2)
        job3 = await self.enqueue("test", timeout=1)
        for _ in range(4):
            job = await self.dequeue()
            job.status = Status.ACTIVE
            job.started = 1000
            await self.queue.update(job)
        await self.dequeue()

        # missing job
        job4 = Job(function="", queue=self.queue)

        # pylint: disable=protected-access
        await self.queue.redis.lpush(self.queue._active, job4.id)

        mock_time.time.return_value = 3
        self.assertEqual(await self.count("active"), 6)
        swept = await self.queue.sweep(abort=0.01)
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
        self.assertEqual(job2.status, Status.QUEUED)
        self.assertEqual(job3.status, Status.QUEUED)
        self.assertEqual(await self.count("active"), 2)

    async def test_job_key(self) -> None:
        self.assertEqual("a", self.queue.job_key_from_id(self.queue.job_id("a")))
        self.assertEqual("a:b", self.queue.job_key_from_id(self.queue.job_id("a:b")))


class TestPostgresQueue(TestQueue):
    async def asyncSetUp(self) -> None:
        self.queue: PostgresQueue = await create_postgres_queue()

    async def test_job_key(self) -> None:
        self.skipTest("Not implemented")

    async def test_map(self) -> None:
        self.skipTest("WIP")

    async def test_apply(self) -> None:
        self.skipTest("WIP")

    async def test_delay(self) -> None:
        self.skipTest("WIP")

    async def test_retry_delay(self) -> None:
        self.skipTest("WIP")

    async def test_stats(self) -> None:
        self.skipTest("WIP")

    async def test_info(self) -> None:
        self.skipTest("WIP")

    @mock.patch("saq.utils.time")
    async def test_schedule(self, mock_time: MagicMock) -> None:
        self.skipTest("WIP")

    async def test_batch(self) -> None:
        with contextlib.suppress(ValueError):
            async with self.queue.batch():
                job = await self.enqueue("echo", a=1)
                raise ValueError()

        self.assertEqual(job.status, Status.ABORTING)

    async def test_enqueue_dup(self) -> None:
        job = await self.enqueue("test", key="1")
        self.assertEqual(job.id, "1")
        self.assertIsNone(await self.queue.enqueue("test", key="1"))
        self.assertIsNone(await self.queue.enqueue(job))

    async def test_abort(self) -> None:
        job = await self.enqueue("test", retries=2)
        self.assertEqual(await self.count("queued"), 1)
        self.assertEqual(await self.count("incomplete"), 1)
        await self.queue.abort(job, "test")
        self.assertEqual(await self.count("queued"), 0)
        self.assertEqual(await self.count("incomplete"), 0)

        job = await self.enqueue("test", retries=2)
        await self.dequeue()
        self.assertEqual(await self.count("queued"), 0)
        self.assertEqual(await self.count("incomplete"), 1)
        self.assertEqual(await self.count("active"), 1)
        await self.queue.abort(job, "test")
        self.assertEqual(await self.count("queued"), 0)
        self.assertEqual(await self.count("incomplete"), 0)
        self.assertEqual(await self.count("active"), 0)
        async with self.queue.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"""
                SELECT status
                FROM {self.queue.jobs_table}
                WHERE key = %s
                """,
                (job.key,),
            )
            self.assertEqual(await cursor.fetchone(), (Status.ABORTING,))

    async def test_sweep(self) -> None:
        job = await self.enqueue("test", ttl=1)
        job = await self.dequeue()
        await self.queue.finish(job, Status.COMPLETE)
        await asyncio.sleep(1)
        await self.queue.sweep()
        with self.assertRaisesRegex(RuntimeError, "doesn't exist"):
            await job.refresh()
