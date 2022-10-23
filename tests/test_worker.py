from __future__ import annotations

import asyncio
import contextvars
import logging
import unittest
import typing as t
from unittest import mock

from saq.job import CronJob, Job, Status
from saq.utils import uuid1
from saq.worker import Worker
from tests.helpers import cleanup_queue, create_queue

if t.TYPE_CHECKING:
    from unittest.mock import MagicMock

    from saq.queue import Queue


logging.getLogger().setLevel(logging.CRITICAL)

ctx_var = contextvars.ContextVar("ctx_var")


async def noop(_ctx: t.Dict[str, t.Union[Worker, Job]]) -> int:
    return 1


async def sleeper(ctx: t.Dict[str, t.Union[Worker, int, Job]]) -> t.Dict[str, int]:
    await asyncio.sleep(ctx.get("sleep", 0.1))
    return {"a": 1, "b": []}


async def cron(_ctx):
    return 1


async def error(_ctx: t.Dict[str, t.Union[Worker, Job]]):
    raise ValueError("oops")


def sync_echo_ctx(_ctx):
    return ctx_var.get()


async def recurse(ctx: t.Dict[str, t.Union[Worker, Job, Queue]], *, n) -> t.List[str]:
    var = ctx_var.get()
    result = [var]
    if n > 0:
        result += await ctx["queue"].apply("recurse", n=n - 1)
    return result


functions = [noop, sleeper, error, sync_echo_ctx, recurse]


class TestWorker(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.queue = create_queue()
        self.worker = Worker(self.queue, functions=functions)

    async def asyncTearDown(self) -> None:
        await cleanup_queue(self.queue)

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

        async def before_process(ctx):
            self.assertIsNotNone(ctx["job"])
            x["before"] += 1

        async def after_process(ctx):
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
        # pylint: disable=protected-access
        await self.queue.redis.delete(self.queue._schedule)
        await worker.schedule()
        self.assertEqual(await self.queue.count("queued"), 1)
        self.assertEqual(await self.queue.count("incomplete"), 1)
        mock_logger.info.assert_any_call("Scheduled %s", [b"saq:job:default:cron:cron"])

    @mock.patch("saq.worker.logger")
    async def test_abort(self, mock_logger: MagicMock) -> None:
        job = await self.enqueue("sleeper")
        self.worker.context["sleep"] = 60
        asyncio.create_task(self.worker.process())

        # wait for the job to actually start
        def callback(job_key, status):
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

        # ensure job doens't get requeued
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
        async def before_process(*_, **__):
            ctx_var.set("123")

        self.worker.before_process = before_process
        asyncio.create_task(self.worker.start())
        self.assertEqual(await self.queue.apply("sync_echo_ctx"), "123")

    async def test_propagation(self) -> None:
        async def before_process(ctx):
            correlation_id = ctx["job"].meta.get("correlation_id")
            ctx_var.set(correlation_id)
            ctx["queue"] = self.queue

        async def before_enqueue(job):
            job.meta["correlation_id"] = ctx_var.get(None) or uuid1()

        self.worker.before_process = before_process
        self.queue.register_before_enqueue(before_enqueue)
        asyncio.create_task(self.worker.start())
        correlation_ids = await self.queue.apply("recurse", n=2)
        self.assertEqual(len(correlation_ids), 3)
        self.assertTrue(all(cid == correlation_ids[0] for cid in correlation_ids[1:]))
