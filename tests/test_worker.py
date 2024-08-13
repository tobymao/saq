from __future__ import annotations

import asyncio
import contextvars
import logging
import typing as t
import unittest
from unittest import mock

from saq.job import CronJob, Job, Status
from saq.queue import Queue
from saq.queue.redis import RedisQueue
from saq.utils import uuid1
from saq.worker import Worker
from tests.helpers import cleanup_queue, create_queue  # create_postgres_queue

if t.TYPE_CHECKING:
    from unittest.mock import MagicMock

    from saq.types import Context, Function


logging.getLogger().setLevel(logging.CRITICAL)

ctx_var = contextvars.ContextVar[str]("ctx_var")


async def noop(_ctx: Context) -> int:
    return 1


async def sleeper(_ctx: Context, sleep: float = 0.1) -> dict[str, t.Any]:
    await asyncio.sleep(sleep)
    return {"a": 1, "b": []}


async def cron(_ctx: Context) -> int:
    return 1


async def error(_ctx: Context) -> t.NoReturn:
    raise ValueError("oops")


def sync_echo_ctx(_ctx: Context) -> str:
    return ctx_var.get()


async def recurse(ctx: Context, *, n: int) -> list[str]:
    var = ctx_var.get()
    result = [var]
    if n > 0:
        result += await ctx["queue"].apply("recurse", n=n - 1)
    return result


functions: list[Function] = [noop, sleeper, error, sync_echo_ctx, recurse]


class TestWorker(unittest.IsolatedAsyncioTestCase):
    queue: Queue
    worker: Worker

    async def asyncSetUp(self) -> None:
        self.skipTest("Skipping base test case")

    async def asyncTearDown(self) -> None:
        await cleanup_queue(self.queue)
        await self.worker.stop()

    async def enqueue(self, function: str, **kwargs: t.Any) -> Job:
        job = await self.queue.enqueue(function, **kwargs)
        assert job is not None
        return job

    async def test_start(self) -> None:
        task = asyncio.create_task(self.worker.start())
        job = await self.enqueue("noop")
        await job.refresh(1)
        self.assertEqual(job.result, 1)
        job = await self.enqueue("error")
        await asyncio.sleep(0.05)
        await job.refresh()
        self.assertEqual(job.status, Status.FAILED)
        assert job.error is not None and "oops" in job.error
        job = await self.enqueue("sleeper")
        self.assertEqual(job.status, Status.QUEUED)
        await asyncio.sleep(0.05)
        await job.refresh()
        self.assertEqual(job.status, Status.ACTIVE)
        task.cancel()
        await asyncio.sleep(0.05)
        await job.refresh()
        self.assertEqual(job.status, Status.QUEUED)

        asyncio.create_task(self.worker.start())
        job = await self.enqueue("noop")
        await job.refresh(1)
        self.assertEqual(job.result, 1)
        job = await self.enqueue("sleeper")
        self.assertEqual(job.status, Status.QUEUED)
        await asyncio.sleep(0.05)
        await job.refresh()
        self.assertEqual(job.status, Status.ACTIVE)
        await self.worker.stop()
        await job.refresh()
        self.assertEqual(job.status, Status.QUEUED)

    async def test_noop(self) -> None:
        job = await self.enqueue("noop")
        assert job.queue != 0
        assert job.started == 0
        assert job.completed == 0
        await self.worker.process()
        await job.refresh()
        assert job.queue != 0
        assert job.started != 0
        assert job.completed != 0
        self.assertEqual(job.status, Status.COMPLETE)
        self.assertEqual(job.result, 1)

    async def test_sleeper(self) -> None:
        job = await self.enqueue("sleeper")
        await self.worker.process()
        await job.refresh()
        assert job.queue != 0
        assert job.started != 0
        assert job.completed != 0
        self.assertEqual(job.status, Status.COMPLETE)
        self.assertEqual(job.result, {"a": 1, "b": []})

        job = await self.enqueue("sleeper", timeout=0.05, retries=2)
        await self.worker.process()
        await self.worker.process()
        await job.refresh()
        self.assertEqual(job.attempts, 2)
        assert job.queue != 0
        assert job.started != 0
        assert job.completed != 0
        self.assertEqual(job.status, Status.FAILED)
        assert job.error is not None and "TimeoutError" in job.error

    async def test_error(self) -> None:
        job = await self.enqueue("error", retries=2)
        await self.worker.process()
        await job.refresh()
        self.assertEqual(job.attempts, 1)
        self.assertEqual(job.result, None)
        assert job.queue != 0
        assert job.started == 0
        assert job.completed == 0
        self.assertEqual(job.status, Status.QUEUED)

        await self.worker.process()
        await job.refresh()
        assert job.queue != 0
        assert job.started != 0
        assert job.completed != 0
        self.assertEqual(job.attempts, 2)
        self.assertEqual(job.status, Status.FAILED)
        assert job.error is not None and 'ValueError("oops")' in job.error

    def test_stop(self) -> None:
        loop = asyncio.new_event_loop()
        queue = create_queue()
        worker = Worker(queue, functions=functions)
        job = loop.run_until_complete(queue.enqueue("sleeper"))
        assert job is not None
        self.assertEqual(job.status, Status.QUEUED)
        worker.tasks.add(loop.create_task(worker.process()))

        loop.run_until_complete(asyncio.sleep(0.05))
        loop.run_until_complete(job.refresh())
        self.assertEqual(job.status, Status.ACTIVE)

        loop.run_until_complete(worker.stop())
        job = loop.run_until_complete(queue.job(job.key))
        assert job is not None
        loop.run_until_complete(cleanup_queue(queue))
        assert job.queued != 0
        assert job.started == 0
        assert job.completed == 0
        self.assertEqual(job.attempts, 1)
        self.assertEqual(job.status, Status.QUEUED)
        self.assertEqual(job.error, "cancelled")
        loop.close()

    async def test_hooks(self) -> None:
        x = {"before": 0, "after": 0}

        async def before_process(ctx: Context) -> None:
            self.assertIsNotNone(ctx["job"])
            x["before"] += 1

        async def after_process(ctx: Context) -> None:
            self.assertIsNotNone(ctx["job"])
            x["after"] += 1

        worker = Worker(
            self.queue,
            functions=functions,
            before_process=before_process,
            after_process=after_process,
        )
        await self.queue.enqueue("noop")
        await worker.process()
        self.assertEqual(x["before"], 1)
        self.assertEqual(x["after"], 1)

        await self.queue.enqueue("error")
        await worker.process()
        self.assertEqual(x["before"], 2)
        self.assertEqual(x["after"], 2)

        task = asyncio.create_task(worker.process())
        await asyncio.sleep(0.05)
        task.cancel()
        self.assertEqual(x["before"], 2)
        self.assertEqual(x["after"], 2)

    async def test_abort_status_after_process(self) -> None:
        status = None
        event = asyncio.Event()

        async def function(_ctx: Context) -> None:
            event.set()
            await asyncio.sleep(float("inf"))

        async def after_process(ctx: Context) -> None:
            nonlocal status
            status = ctx["job"].status

        worker = Worker(
            self.queue,
            functions=[("function", function)],
            after_process=after_process,
        )
        job = await self.enqueue("function")

        task = asyncio.create_task(worker.process())
        await event.wait()
        await job.abort("aborted")
        await worker.abort(0)
        await task
        self.assertEqual(Status.ABORTED, status)

    @mock.patch("saq.utils.time")
    async def test_schedule(self, mock_time: MagicMock) -> None:
        mock_time.time.return_value = 1
        job = await self.enqueue("noop", scheduled=2)
        self.assertEqual(await self.queue.count("queued"), 0)
        mock_time.time.return_value = 3
        await self.queue.schedule()
        await self.worker.process()
        await job.refresh()
        self.assertEqual(job.result, 1)

    @mock.patch("saq.worker.logger")
    @mock.patch("saq.utils.time")
    async def test_cron(self, mock_time: MagicMock, mock_logger: MagicMock) -> None:
        with self.assertRaises(ValueError):
            Worker(
                self.queue,
                functions=functions,
                cron_jobs=[CronJob(cron, cron="x")],
            )

        mock_time.time.return_value = 1
        worker = Worker(
            self.queue,
            functions=functions,
            cron_jobs=[CronJob(cron, cron="* * * * *")],
        )
        self.assertEqual(await self.queue.count("queued"), 0)
        self.assertEqual(await self.queue.count("incomplete"), 0)
        await worker.schedule()
        self.assertEqual(await self.queue.count("queued"), 0)
        self.assertEqual(await self.queue.count("incomplete"), 1)

        mock_time.time.return_value = 60
        if isinstance(self.queue, RedisQueue):
            # pylint: disable=protected-access
            await self.queue.redis.delete(self.queue._schedule)
        await worker.schedule()
        self.assertEqual(await self.queue.count("queued"), 1)
        self.assertEqual(await self.queue.count("incomplete"), 1)
        # Remove if statement when schedule is implemented for Postgres queue
        if isinstance(self.queue, RedisQueue):
            mock_logger.info.assert_any_call(
                "Scheduled %s", [b"saq:job:default:cron:cron"]
            )

    @mock.patch("saq.worker.logger")
    async def test_abort(self, mock_logger: MagicMock) -> None:
        job = await self.enqueue("sleeper")
        self.worker.context["sleep"] = 60
        asyncio.create_task(self.worker.process())

        # wait for the job to actually start
        def callback(job_key: str, status: Status) -> bool:
            self.assertEqual(job.key, job_key)
            self.assertEqual(status, Status.ACTIVE)
            return True

        await self.queue.listen([job.key], callback)
        self.assertEqual(await self.queue.count("queued"), 0)
        self.assertEqual(await self.queue.count("incomplete"), 1)
        self.assertEqual(await self.queue.count("active"), 1)
        await job.abort("test")
        self.assertEqual(await self.queue.count("queued"), 0)
        self.assertEqual(await self.queue.count("incomplete"), 0)
        self.assertEqual(await self.queue.count("active"), 0)

        # ensure job doesn't get requeued
        await job.enqueue()
        self.assertEqual(await self.queue.count("queued"), 0)
        self.assertEqual(await self.queue.count("incomplete"), 0)
        self.assertEqual(await self.queue.count("active"), 0)

        await self.worker.abort(0.0001)
        mock_logger.info.assert_any_call("Aborting %s", job.id)
        await job.refresh()
        self.assertEqual(await self.queue.count("queued"), 0)
        self.assertEqual(await self.queue.count("incomplete"), 0)
        self.assertEqual(await self.queue.count("active"), 0)
        self.assertEqual(job.status, Status.ABORTED)
        self.assertEqual(job.error, "test")

        # ensure job can get requeued
        await job.enqueue()
        self.assertEqual(await self.queue.count("queued"), 1)
        self.assertEqual(await self.queue.count("incomplete"), 1)
        self.assertEqual(await self.queue.count("active"), 0)

    async def test_sync_function(self) -> None:
        async def before_process(*_: t.Any, **__: t.Any) -> None:
            ctx_var.set("123")

        self.worker.before_process = before_process
        asyncio.create_task(self.worker.start())
        self.assertEqual(await self.queue.apply("sync_echo_ctx"), "123")

    async def test_propagation(self) -> None:
        async def before_process(ctx: Context) -> None:
            correlation_id = ctx["job"].meta.get("correlation_id")
            ctx_var.set(correlation_id)  # type: ignore
            ctx["queue"] = self.queue

        async def before_enqueue(job: Job) -> None:
            job.meta["correlation_id"] = ctx_var.get(None) or uuid1()

        self.worker.before_process = before_process
        self.queue.register_before_enqueue(before_enqueue)
        asyncio.create_task(self.worker.start())
        correlation_ids = await self.queue.apply("recurse", n=2)
        self.assertEqual(len(correlation_ids), 3)
        self.assertTrue(all(cid == correlation_ids[0] for cid in correlation_ids[1:]))

    async def test_sweep_abort(self) -> None:
        state = {"counter": 0}

        async def handler(_ctx: Context) -> None:
            await asyncio.sleep(3)
            state["counter"] += 1

        self.worker = Worker(
            self.queue, functions=[("handler", handler)], timers={"sweep": 1}  # type: ignore
        )
        asyncio.create_task(self.worker.start())
        await self.queue.enqueue("handler", heartbeat=1, retries=2)
        await asyncio.sleep(6)
        self.assertEqual(state["counter"], 0)


class TestWorkerRedisQueue(TestWorker):
    async def asyncSetUp(self) -> None:
        self.queue = create_queue()
        self.worker = Worker(self.queue, functions=functions)


class TestWorkerPostgresQueue(TestWorker):
    async def asyncSetUp(self) -> None:
        # self.queue = await create_postgres_queue()
        # self.worker = Worker(self.queue, functions=functions)
        self.skipTest("Skipping Postgres test case")
