from __future__ import annotations

import asyncio
import contextlib
import pickle
import time
import typing as t
import unittest
from functools import partial
from unittest import mock

from psycopg.sql import SQL

from saq.errors import InvalidUrlError
from saq.job import Job, Status
from saq.queue import JobError, Queue
from saq.queue.postgres import PostgresQueue
from saq.utils import uuid1
from saq.worker import Worker
from tests.helpers import (
    cleanup_queue,
    create_postgres_queue,
    create_redis_queue,
    setup_postgres,
    teardown_postgres,
)


if t.TYPE_CHECKING:
    from unittest.mock import MagicMock

    from saq.queue.redis import RedisQueue
    from saq.types import Context, CountKind, Function


async def echo(_ctx: Context, *, a: int) -> int:
    return a


async def error(_ctx: Context) -> None:
    raise ValueError("oops")


functions: list[Function] = [echo, error]


class TestQueueError(unittest.IsolatedAsyncioTestCase):
    queue: Queue
    create_queue: t.Callable

    async def asyncSetUp(self) -> None:
        self.create_queue = partial(create_redis_queue, "scheme://localhost")

    async def test_queue(self) -> None:
        with self.assertRaises(InvalidUrlError):
            self.queue: RedisQueue = await self.create_queue()


class TestQueue(unittest.IsolatedAsyncioTestCase):
    queue: Queue
    create_queue: t.Callable

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
        await asyncio.sleep(0.1)
        await self.enqueue("test")
        await task
        self.assertEqual(await self.count("queued"), 0)

    async def test_dequeue_fifo(self) -> None:
        await cleanup_queue(self.queue)
        self.queue = await self.create_queue()
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
        worker = Worker(self.queue, functions=functions, dequeue_timeout=0.1)
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
        worker = Worker(self.queue, functions=functions)
        for _ in range(10):
            await self.enqueue("test")
            job = await self.dequeue()
            await job.retry(None)
            await job.finish(Status.ABORTED)
            await job.finish(Status.FAILED)
            await job.finish(Status.COMPLETE)
        worker_info = await worker.worker_info()
        self.assertEqual(worker_info["stats"]["complete"], 10)
        self.assertEqual(worker_info["stats"]["failed"], 10)
        self.assertEqual(worker_info["stats"]["retried"], 10)
        self.assertEqual(worker_info["stats"]["aborted"], 10)
        self.assertGreater(worker_info["stats"]["uptime"], 0)

    async def test_info(self) -> None:
        queue2 = await self.create_queue(name=self.queue.name)
        self.addAsyncCleanup(cleanup_queue, queue2)
        worker = Worker(
            self.queue,
            functions=functions,
            metadata={"foo": "bar"},
        )
        info = await self.queue.info(jobs=True)
        self.assertEqual(info["workers"], {})
        self.assertEqual(info["active"], 0)
        self.assertEqual(info["queued"], 0)
        self.assertEqual(info["scheduled"], 0)
        self.assertEqual(info["jobs"], [])

        await self.enqueue("echo", a=1)
        await queue2.enqueue("echo", a=1)
        await worker.process()
        await worker.worker_info(3)

        info = await self.queue.info(jobs=True)
        self.assertEqual(set(info["workers"].keys()), {worker.id})
        self.assertEqual(info["workers"][worker.id]["metadata"], {"foo": "bar"})
        self.assertEqual(info["workers"][worker.id]["queue_key"], self.queue.name)
        assert info["workers"][worker.id]["stats"] is not None
        self.assertEqual(info["active"], 0)
        self.assertEqual(info["queued"], 1)
        self.assertEqual(len(info["jobs"]), 1)

        time.sleep(4)
        info = await self.queue.info(jobs=True)
        self.assertEqual(info["workers"], {})
        await worker.queue.sweep()
        info = await self.queue.info(jobs=True)
        self.assertEqual(info["workers"], {})

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
        self.assertEqual(jobs1, [job1.id, job2.id])
        self.assertEqual(jobs2, [])
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

        task = asyncio.create_task(self.queue.listen([job.key], listen, timeout=1))
        await asyncio.sleep(0.1)
        await self.queue.update(job)
        self.assertEqual(job.status, Status.QUEUED)
        job.status = Status.ACTIVE
        await self.queue.update(job)
        # job shouldn't mutate
        self.assertEqual(job.status, Status.ACTIVE)
        # status is actually queued
        self.assertEqual((await self.queue.job(job.key)).status, Status.QUEUED)
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
        self.assertEqual(await self.queue.map("echo", [{}, {"a": 2}], timeout=10, a=3), [3, 2])
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

    async def test_iter_jobs(self) -> None:
        for _ in range(9):
            await self.queue.enqueue("test")

        async for job in self.queue.iter_jobs(batch_size=1):
            await job.update(status=Status.ACTIVE)
            break

        self.assertEqual(9, len([job async for job in self.queue.iter_jobs()]))
        self.assertEqual(9, len([job async for job in self.queue.iter_jobs(batch_size=1)]))
        self.assertEqual(9, len([job async for job in self.queue.iter_jobs(batch_size=2)]))
        self.assertEqual(9, len([job async for job in self.queue.iter_jobs(batch_size=3)]))
        self.assertEqual(
            1, len([job async for job in self.queue.iter_jobs(statuses=[Status.ACTIVE])])
        )

    async def test_update_refresh(self) -> None:
        # checks for a race condition in refresh which replaces the jobs data against an update
        # we want to make sure the update doesn't get overriden halfway in the async flow
        job = await self.queue.enqueue("test")
        self.assertEqual(job.touched, 0)
        asyncio.create_task(job.refresh())
        await job.update()
        job = await self.queue.job(job.key)
        self.assertGreater(job.touched, 0)


class TestRedisQueue(TestQueue):
    async def asyncSetUp(self) -> None:
        self.create_queue = create_redis_queue
        self.queue: RedisQueue = await self.create_queue()

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
            job.started = 1000
            await self.queue.update(job, status=Status.ACTIVE)
        await self.dequeue()

        # missing job
        job4 = Job(function="", queue=self.queue)

        await self.queue.redis.lpush(self.queue._active, job4.id)

        mock_time.time.return_value = 3
        self.assertEqual(await self.count("active"), 6)
        swept = await self.queue.sweep(abort=0.01)
        self.assertEqual(
            set(swept),
            {
                job1.id,
                job2.id,
                job3.id,
                job4.id,
            },
        )
        await job1.refresh()
        await job2.refresh()
        await job3.refresh()
        self.assertEqual(job1.status, Status.ABORTED)
        self.assertEqual(job1.error, "swept")
        self.assertEqual(job2.status, Status.QUEUED)
        self.assertEqual(job3.status, Status.QUEUED)
        self.assertEqual(await self.count("active"), 2)

    async def test_job_key(self) -> None:
        self.assertEqual("a", self.queue.job_key_from_id(self.queue.job_id("a")))
        self.assertEqual("a:b", self.queue.job_key_from_id(self.queue.job_id("a:b")))


class TestPostgresQueue(TestQueue):
    async def asyncSetUp(self) -> None:
        await setup_postgres()
        self.create_queue = create_postgres_queue
        self.queue: PostgresQueue = await self.create_queue()

    async def asyncTearDown(self) -> None:
        await super().asyncTearDown()
        await teardown_postgres()

    @unittest.skip("Not implemented")
    async def test_job_key(self) -> None:
        pass

    @unittest.skip("Not implemented")
    @mock.patch("saq.utils.time")
    async def test_schedule(self, mock_time: MagicMock) -> None:
        pass

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
        await job.refresh()
        self.assertEqual(job.status, Status.ABORTED)
        self.assertEqual(await self.queue.get_job_status(job.key), Status.ABORTED)

        job = await self.enqueue("test", retries=2)
        await self.dequeue()
        self.assertEqual(await self.count("queued"), 0)
        self.assertEqual(await self.count("incomplete"), 1)
        self.assertEqual(await self.count("active"), 1)
        await self.queue.abort(job, "test")
        self.assertEqual(await self.count("queued"), 0)
        self.assertEqual(await self.count("incomplete"), 0)
        self.assertEqual(await self.count("active"), 0)
        self.assertEqual(await self.queue.get_job_status(job.key), Status.ABORTING)

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
            job.started = 1000
            await self.queue.update(job, status=Status.ACTIVE)

        # job3 has been dequeued but the job state hasn't been updated
        await self.dequeue()

        mock_time.time.return_value = 3
        self.assertEqual(await self.count("active"), 5)
        swept = await self.queue.sweep(abort=0.01)
        self.assertEqual(
            set(swept),
            {
                job1.key,
                job2.key,
                job3.key,
            },
        )
        await job1.refresh()
        await job2.refresh()
        await job3.refresh()
        self.assertEqual(job1.status, Status.ABORTED)
        self.assertEqual(job1.error, "swept")
        self.assertEqual(job2.status, Status.QUEUED)
        self.assertEqual(job3.status, Status.QUEUED)
        self.assertEqual(await self.count("active"), 2)

    @mock.patch("saq.utils.time")
    async def test_sweep_stuck(self, mock_time: MagicMock) -> None:
        mock_time.time.return_value = 0
        job1 = await self.queue.enqueue("test")
        assert job1
        job = await self.dequeue()
        job.started = 1000
        await self.queue.update(job, status=Status.ACTIVE)

        # Enqueue 2 more jobs that will become stuck
        job2 = await self.queue.enqueue("test", retries=0)
        assert job2
        job3 = await self.queue.enqueue("test")
        assert job3

        another_queue = await self.create_queue()
        for _ in range(2):
            job = await another_queue.dequeue()
            await another_queue.update(job, status=Status.ACTIVE, started=1000)

        # Disconnect another_queue to simulate worker going down
        await another_queue.disconnect()

        mock_time.time.return_value = 3
        self.assertEqual(await self.count("active"), 3)
        swept = await self.queue.sweep(abort=0.01)
        self.assertEqual(
            set(swept),
            {
                job2.id,
                job3.id,
            },
        )
        await job1.refresh()
        await job2.refresh()
        await job3.refresh()
        self.assertEqual(job1.status, Status.ACTIVE)
        self.assertEqual(job2.status, Status.ABORTED)
        self.assertEqual(job3.status, Status.QUEUED)
        self.assertEqual(await self.count("active"), 1)

    @mock.patch("saq.utils.time")
    async def test_sweep_stuck_lock(self, mock_time: MagicMock) -> None:
        mock_time.time.return_value = 0
        job1 = await self.queue.enqueue("test")
        assert job1
        job = await self.dequeue()
        await self.queue.update(job, status=Status.ACTIVE, started=0)
        await self.queue.disconnect()

        another_queue = await self.create_queue()
        another_queue.job_lock_sweep = False

        mock_time.time.return_value = 3
        swept = await another_queue.sweep(abort=0.01)
        self.assertEqual(set(swept), set())

    async def test_sweep_jobs(self) -> None:
        job1 = await self.enqueue("test", ttl=1)
        job2 = await self.enqueue("test", ttl=60)
        await self.queue.finish(job1, Status.COMPLETE)
        await self.queue.finish(job2, Status.COMPLETE)
        await asyncio.sleep(1.5)

        await self.queue.sweep()
        with self.assertRaisesRegex(RuntimeError, "doesn't exist"):
            await job1.refresh()
        await job2.refresh()
        self.assertEqual(job2.status, Status.COMPLETE)

    async def test_sweep_stats(self) -> None:
        worker = Worker(self.queue, functions=functions)
        # Stats are deleted
        await worker.worker_info(ttl=1)
        await asyncio.sleep(1.5)
        await self.queue.sweep()
        async with self.queue.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                SQL(
                    """
                SELECT stats
                FROM {}
                WHERE worker_id = %s
                """
                ).format(self.queue.stats_table),
                (worker.id,),
            )
            self.assertIsNone(await cursor.fetchone())

        # Stats are not deleted
        await worker.worker_info(ttl=60)
        await asyncio.sleep(1)
        await self.queue.sweep()
        async with self.queue.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                SQL(
                    """
                SELECT stats
                FROM {}
                WHERE worker_id = %s
                """
                ).format(self.queue.stats_table),
                (worker.id,),
            )
            self.assertIsNotNone(await cursor.fetchone())

    async def test_job_lock(self) -> None:
        query = SQL(
            """
        SELECT count(*)
        FROM {} JOIN pg_locks ON lock_key = objid
        WHERE key = %(key)s
          AND classid = {}
          AND objsubid = 2 -- key is int pair, not single bigint
        """
        ).format(self.queue.jobs_table, self.queue.job_lock_keyspace)
        job = await self.enqueue("test")
        await self.dequeue()
        async with self.queue.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(query, {"key": job.key})
            self.assertEqual(await cursor.fetchone(), (1,))

        await self.finish(job, Status.COMPLETE, result=1)
        async with self.queue.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(query, {"key": job.key})
            self.assertEqual(await cursor.fetchone(), (0,))

    async def test_load_dump_pickle(self) -> None:
        self.queue = await self.create_queue(dump=pickle.dumps, load=pickle.loads)
        job = await self.enqueue("test")

        async with self.queue.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                SQL(
                    """
                SELECT job
                FROM {}
                WHERE key = %s
                """
                ).format(self.queue.jobs_table),
                (job.key,),
            )
            result = await cursor.fetchone()
            assert result
            fetched_job = pickle.loads(result[0])
            self.assertIsInstance(fetched_job, dict)
            self.assertEqual(fetched_job["key"], job.key)

        dequeued_job = await self.dequeue()
        self.assertEqual(dequeued_job, job)

    @mock.patch("saq.utils.time")
    async def test_finish_ttl_positive(self, mock_time: MagicMock) -> None:
        mock_time.time.return_value = 0
        job = await self.enqueue("test", ttl=5)
        await self.dequeue()
        await self.finish(job, Status.COMPLETE)
        async with self.queue.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                SQL(
                    """
                SELECT expire_at
                FROM {}
                WHERE key = %s
                """
                ).format(self.queue.jobs_table),
                (job.key,),
            )
            result = await cursor.fetchone()
            self.assertEqual(result, (5,))

    @mock.patch("saq.utils.time")
    async def test_finish_ttl_neutral(self, mock_time: MagicMock) -> None:
        mock_time.time.return_value = 0
        job = await self.enqueue("test", ttl=0)
        await self.dequeue()
        await self.finish(job, Status.COMPLETE)
        async with self.queue.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                SQL(
                    """
                SELECT expire_at
                FROM {}
                WHERE key = %s
                """
                ).format(self.queue.jobs_table),
                (job.key,),
            )
            result = await cursor.fetchone()
            self.assertEqual(result, (None,))

    @mock.patch("saq.utils.time")
    async def test_finish_ttl_negative(self, mock_time: MagicMock) -> None:
        mock_time.time.return_value = 0
        job = await self.enqueue("test", ttl=-1)
        await self.dequeue()
        await self.finish(job, Status.COMPLETE)
        async with self.queue.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                SQL(
                    """
                SELECT expire_at
                FROM {}
                WHERE key = %s
                """
                ).format(self.queue.jobs_table),
                (job.key,),
            )
            result = await cursor.fetchone()
            self.assertIsNone(result)

    @mock.patch("saq.utils.time")
    async def test_cron_job_close_to_target(self, mock_time: MagicMock) -> None:
        mock_time.time.return_value = 1000.5
        await self.enqueue("test", scheduled=1001)
        # The job is scheduled to run at 1001, but we're running at 1000.5
        # so it should not be picked up
        job = await self.queue.dequeue(timeout=1)
        assert not job

    async def test_bad_connection(self) -> None:
        job = await self.enqueue("test")

        async with self.queue._get_dequeue_conn() as original_connection:
            pass

        await original_connection.close()
        # Test dequeue still works
        self.assertEqual((await self.dequeue()), job)
        # Check queue has a new connection
        self.assertNotEqual(original_connection, self.queue._dequeue_conn)

        await self.queue.pool.putconn(original_connection)

    async def test_group_key(self) -> None:
        job1 = await self.enqueue("test", group_key=1)
        assert job1
        job2 = await self.enqueue("test", group_key=1)
        assert job2
        self.assertEqual(await self.count("queued"), 2)

        assert await self.dequeue()
        self.assertEqual(await self.count("queued"), 1)
        assert not await self.queue.dequeue(0.01)
        await job1.update(status=Status.COMPLETE)
        assert await self.dequeue()

    async def test_priority(self) -> None:
        assert await self.enqueue("test", priority=-1)
        self.assertEqual(await self.count("queued"), 1)
        assert not await self.queue.dequeue(0.01)

    async def test_repeated_connect_disconnect(self) -> None:
        mock_pool = mock.AsyncMock()
        queue = PostgresQueue(url="fake_url")
        queue.pool = mock_pool

        with mock.patch.object(queue, "init_db", return_value=None) as mock_init_db:
            await queue.connect()
            await queue.connect()

            mock_pool.open.assert_called_once()
            mock_init_db.assert_called_once()

            await queue.disconnect()
            await queue.disconnect()

            mock_pool.close.assert_called_once()

    async def test_lock_skip(self) -> None:
        await self.queue.enqueue("test")
        job = await self.queue.dequeue()
        await self.queue.retry(job, "")

        queue2 = await self.create_queue()
        await queue2.enqueue("test")
        job2 = await queue2.dequeue()
        assert job2.key == job.key
