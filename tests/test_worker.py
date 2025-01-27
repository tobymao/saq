from __future__ import annotations

import asyncio
import contextvars
import logging
import time
import typing as t
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase
from time_machine import travel

from saq.job import CronJob, Job, Status, ACTIVE_STATUSES
from saq.queue import Queue
from saq.queue.http import HttpProxy
from saq.queue.redis import RedisQueue
from saq.utils import uuid1
from saq.worker import Worker
from tests.helpers import (
    cleanup_queue,
    create_redis_queue,
    create_postgres_queue,
    setup_postgres,
    teardown_postgres,
)

if t.TYPE_CHECKING:
    from unittest.mock import MagicMock

    from saq.types import Context, Function


logging.getLogger().setLevel(logging.INFO)

ctx_var = contextvars.ContextVar[str]("ctx_var")


async def noop(_ctx: Context) -> int:
    return 1


async def sleeper(_ctx: Context, sleep: float = 0.1) -> dict[str, t.Any]:
    await asyncio.sleep(sleep)
    return {"a": 1, "b": []}


async def cron(_ctx: Context, *, param: int) -> int:
    return param


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
    create_queue: t.Callable

    async def asyncSetUp(self) -> None:
        self.skipTest("Skipping base test case")

    async def asyncTearDown(self) -> None:
        await self.worker.stop()
        await cleanup_queue(self.queue)

    async def enqueue(self, function: str, **kwargs: t.Any) -> Job:
        job = await self.queue.enqueue(function, **kwargs)
        assert job is not None
        return job

    @mock.patch("saq.worker.logger")
    async def test_start(self, _mock_logger: MagicMock) -> None:
        task = asyncio.create_task(self.worker.start())
        job = await self.enqueue("noop")
        await job.refresh(0)
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
        await task
        await job.refresh()
        self.assertEqual(job.status, Status.QUEUED)

        asyncio.create_task(self.worker.start())
        job = await self.enqueue("noop")
        await job.refresh(0)
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

    @mock.patch("saq.worker.logger")
    async def test_sleeper(self, _mock_logger: MagicMock) -> None:
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

    @mock.patch("saq.worker.logger")
    async def test_error(self, _mock_logger: MagicMock) -> None:
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
        queue = loop.run_until_complete(self.create_queue())
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

    @mock.patch("saq.worker.logger")
    async def test_hooks(self, _mock_logger: MagicMock) -> None:
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

    @mock.patch("saq.worker.logger")
    async def test_hooks_many(self, _mock_logger: MagicMock) -> None:
        x = {"before_a": 0, "before_b": 0, "after_a": 0, "after_b": 0}

        async def before_process_a(ctx: Context) -> None:
            self.assertIsNotNone(ctx["job"])
            x["before_a"] += 1

        def before_process_b(ctx: Context) -> None:
            self.assertIsNotNone(ctx["job"])
            x["before_b"] += 1

        async def after_process_a(ctx: Context) -> None:
            self.assertIsNotNone(ctx["job"])
            x["after_a"] += 1

        def after_process_b(ctx: Context) -> None:
            self.assertIsNotNone(ctx["job"])
            x["after_b"] += 1

        worker = Worker(
            self.queue,
            functions=functions,
            before_process=[before_process_a, before_process_b],
            after_process=[after_process_a, after_process_b],
        )
        await self.queue.enqueue("noop")
        await worker.process()
        self.assertEqual(x["before_a"], 1)
        self.assertEqual(x["before_b"], 1)
        self.assertEqual(x["after_a"], 1)
        self.assertEqual(x["after_b"], 1)

        await self.queue.enqueue("error")
        await worker.process()
        self.assertEqual(x["before_a"], 2)
        self.assertEqual(x["before_b"], 2)
        self.assertEqual(x["after_a"], 2)
        self.assertEqual(x["after_b"], 2)

        task = asyncio.create_task(worker.process())
        await asyncio.sleep(0.05)
        task.cancel()
        self.assertEqual(x["before_a"], 2)
        self.assertEqual(x["before_b"], 2)
        self.assertEqual(x["after_a"], 2)
        self.assertEqual(x["after_b"], 2)

    @mock.patch("saq.worker.logger")
    async def test_startup_shutdown_hooks_many(self, _mock_logger: MagicMock) -> None:
        x = {
            "startup_a": 0,
            "startup_b": 0,
            "shutdown_a": 0,
            "shutdown_b": 0,
        }

        async def startup_a(ctx: Context) -> None:
            x["startup_a"] += 1

        def startup_b(ctx: Context) -> None:
            x["startup_b"] += 1

        async def shutdown_a(ctx: Context) -> None:
            x["shutdown_a"] += 1

        def shutdown_b(ctx: Context) -> None:
            x["shutdown_b"] += 1

        worker = Worker(
            self.queue,
            functions=functions,
            startup=[startup_a, startup_b],
            shutdown=[shutdown_a, shutdown_b],
        )

        task = asyncio.create_task(worker.start())
        job = await self.enqueue("noop")
        await job.refresh(0)
        self.assertEqual(job.result, 1)
        self.assertEqual(x["startup_a"], 1)
        self.assertEqual(x["startup_b"], 1)
        self.assertEqual(x["shutdown_a"], 0)
        self.assertEqual(x["shutdown_b"], 0)

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        self.assertEqual(x["startup_a"], 1)
        self.assertEqual(x["startup_b"], 1)
        self.assertEqual(x["shutdown_a"], 1)
        self.assertEqual(x["shutdown_b"], 1)

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
    async def test_cron(self, mock_logger: MagicMock) -> None:
        with self.assertRaises(ValueError):
            Worker(
                self.queue,
                functions=functions,
                cron_jobs=[CronJob(sleeper, cron="x")],
            )

        with travel(datetime(2025, 1, 1, 0, 0, 1, tzinfo=timezone.utc), tick=True) as traveller:
            worker = Worker(
                self.queue,
                functions=functions,
                cron_jobs=[CronJob(sleeper, cron="* 12 * * *", kwargs={"sleep": 5})],
                cron_tz=timezone(offset=timedelta(hours=-3)),  # 3 hours behind (UTC-3)
            )
            await worker.queue.connect()
            self.assertEqual(await self.queue.count("incomplete"), 0)
            asyncio.create_task(worker.start())
            await asyncio.sleep(1)
            self.assertEqual(await self.queue.count("incomplete"), 1)

            traveller.move_to(
                datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            )  # noon UTC
            await asyncio.sleep(2)
            self.assertEqual(await self.queue.count("active"), 0)

            traveller.move_to(
                datetime(2025, 1, 1, 15, 0, 0, tzinfo=timezone.utc)
            )  # noon in tz
            await asyncio.sleep(2)
            self.assertEqual(await self.queue.count("active"), 1)

            # Remove if statement when schedule is implemented for Postgres queue
            if isinstance(self.queue, RedisQueue):
                mock_logger.info.assert_any_call(
                    "Scheduled %s", ["saq:job:default:cron:sleeper"]
                )

    @mock.patch("saq.worker.logger")
    async def test_abort(self, mock_logger: MagicMock) -> None:
        job = await self.enqueue("sleeper", sleep=60)

        # wait for the job to actually start
        def callback(job_key: str, status: Status) -> bool:
            self.assertEqual(job.key, job_key)
            return status == Status.ACTIVE

        listen = asyncio.create_task(self.queue.listen([job.key], callback))
        await asyncio.sleep(0)
        asyncio.create_task(self.worker.process())
        await listen

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

        await job.enqueue()
        self.assertEqual(await self.queue.count("queued"), 1)
        self.assertEqual(await self.queue.count("incomplete"), 1)
        self.assertEqual(await self.queue.count("active"), 0)

    async def test_sync_function(self) -> None:
        async def before_process(*_: t.Any, **__: t.Any) -> None:
            ctx_var.set("123")

        self.worker.before_process = [before_process]
        task = asyncio.create_task(self.worker.start())
        self.assertEqual(await self.queue.apply("sync_echo_ctx"), "123")
        task.cancel()

    async def test_propagation(self) -> None:
        async def before_process(ctx: Context) -> None:
            correlation_id = ctx["job"].meta.get("correlation_id")
            ctx_var.set(correlation_id)  # type: ignore
            ctx["queue"] = self.queue

        async def before_enqueue(job: Job) -> None:
            job.meta["correlation_id"] = ctx_var.get(None) or uuid1()

        self.worker.before_process = [before_process]
        self.queue.register_before_enqueue(before_enqueue)
        asyncio.create_task(self.worker.start())
        correlation_ids = await self.queue.apply("recurse", n=2)
        self.assertEqual(len(correlation_ids), 3)
        self.assertTrue(all(cid == correlation_ids[0] for cid in correlation_ids[1:]))

    async def test_sweep_abort(self) -> None:
        state = {"counter": 0}

        async def handler(_ctx: Context) -> None:
            await asyncio.sleep(4)
            state["counter"] += 1

        self.worker = Worker(
            self.queue,
            functions=[("handler", handler)],
            timers={"sweep": 1},  # type: ignore
        )
        asyncio.create_task(self.worker.start())
        await self.queue.enqueue("handler", heartbeat=1, retries=2)
        await asyncio.sleep(6)
        self.assertEqual(state["counter"], 0)

    async def test_cron_solo_worker(self) -> None:
        state = {"counter": 0}

        async def handler(_ctx: Context) -> None:
            state["counter"] += 1

        self.worker = Worker(
            self.queue,
            functions=[],
            cron_jobs=[CronJob(handler, cron="* * * * * */1")],
            concurrency=1,
        )
        asyncio.create_task(self.worker.start())
        await asyncio.sleep(2)
        self.assertGreater(state["counter"], 0)

    async def test_info(self) -> None:
        info = await self.queue.info()
        assert info

    async def test_burst(self) -> None:
        with self.assertRaises(ValueError):
            # dequeue_timeout must be set when burst is True
            Worker(self.queue, functions=functions, burst=True)

        worker = Worker(
            self.queue, functions=functions, burst=True, dequeue_timeout=0.1, concurrency=1
        )
        worker_task = asyncio.create_task(worker.start())
        job_a = await self.enqueue("noop")
        job_b = await self.enqueue("noop")
        await job_a.refresh(0)
        await job_b.refresh(0)
        self.assertEqual(job_a.status, Status.COMPLETE)
        self.assertEqual(job_b.status, Status.COMPLETE)

        await asyncio.sleep(0.5)
        self.assertTrue(worker.event.is_set())
        await worker_task

    async def test_max_burst_jobs(self) -> None:
        worker = Worker(
            self.queue,
            functions=functions,
            burst=True,
            max_burst_jobs=1,
            dequeue_timeout=0.1,
        )
        worker_task = asyncio.create_task(worker.start())
        job_a = await self.enqueue("noop")
        job_b = await self.enqueue("noop")
        job_c = await self.enqueue("noop")

        await asyncio.sleep(0.5)
        self.assertTrue(worker.event.is_set())
        await worker_task

        await job_a.refresh()
        await job_b.refresh()
        await job_c.refresh()
        self.assertEqual(job_a.status, Status.COMPLETE)
        self.assertEqual(job_b.status, Status.QUEUED)
        self.assertEqual(job_c.status, Status.QUEUED)

    async def test_sync_cancel(self) -> None:
        state = {"counter": 0}

        def work(ctx=None):
            time.sleep(0.2)
            if ctx:
                job = ctx["job"]
                asyncio.run(job.refresh())
                if job.status not in ACTIVE_STATUSES:
                    return
            state["counter"] += 1

        def no_cancel(ctx):
            work()

        def yes_cancel(ctx):
            work(ctx)

        worker = Worker(
            self.queue,
            functions=[
                ("no_cancel", no_cancel),
                ("yes_cancel", yes_cancel),
            ],
        )

        job = await self.enqueue("yes_cancel")
        asyncio.create_task(worker.process())
        await asyncio.sleep(0.1)
        await job.update(status=Status.ABORTING)
        await worker.abort(0)
        await job.refresh()
        self.assertEqual(job.status, Status.ABORTED)
        self.assertEqual(state["counter"], 0)

        job = await self.enqueue("no_cancel")
        asyncio.create_task(worker.process())
        await asyncio.sleep(0.1)
        await job.update(status=Status.ABORTING)
        await worker.abort(0)
        await job.refresh()
        self.assertEqual(job.status, Status.ABORTED)
        self.assertEqual(state["counter"], 1)


class TestWorkerRedisQueue(TestWorker):
    async def asyncSetUp(self) -> None:
        self.create_queue = create_redis_queue
        self.queue = await self.create_queue()
        self.worker = Worker(self.queue, functions=functions)


class TestWorkerPostgresQueue(TestWorker):
    async def asyncSetUp(self) -> None:
        await setup_postgres()
        self.create_queue = create_postgres_queue
        self.queue = await self.create_queue()
        self.worker = Worker(self.queue, functions=functions)

    async def asyncTearDown(self) -> None:
        await super().asyncTearDown()
        await teardown_postgres()

    @mock.patch("saq.utils.time")
    async def test_schedule(self, mock_time: MagicMock) -> None:
        self.skipTest("Not implemented")

    @mock.patch("saq.worker.logger")
    async def test_cron(self, mock_logger: MagicMock) -> None:
        with self.assertRaises(ValueError):
            Worker(
                self.queue,
                functions=functions,
                cron_jobs=[CronJob(cron, cron="x")],
            )

        worker = Worker(
            self.queue,
            functions=functions,
            cron_jobs=[CronJob(cron, cron="* * * * * *")],
        )
        self.assertEqual(await self.queue.count("queued"), 0)
        self.assertEqual(await self.queue.count("incomplete"), 0)
        await worker.schedule()
        self.assertEqual(await self.queue.count("incomplete"), 1)
        await asyncio.sleep(1)

        self.assertEqual(await self.queue.count("queued"), 1)
        self.assertEqual(await self.queue.count("incomplete"), 1)


class TestWorkerHttpQueue(AioHTTPTestCase, TestWorker):
    async def asyncSetUp(self) -> None:
        self.redis_queue = await create_redis_queue()
        self.redis_worker = Worker(
            self.redis_queue,
            functions={},
            concurrency=0,
            timers={"sweep": 1},  # type: ignore
        )

        await super().asyncSetUp()

        async def create_http_queue():
            queue = Queue.from_url("")
            queue.session = self.client
            return queue

        self.create_queue = create_http_queue
        self.queue = await self.create_queue()
        self.worker = Worker(self.queue, functions=functions)

    async def asyncTearDown(self) -> None:
        await super().asyncTearDown()
        await self.redis_worker.stop()
        await self.redis_queue.disconnect()

    async def get_application(self):
        """
        Override the get_app method to return your application.
        """
        proxy = HttpProxy(self.redis_queue)

        async def startup(_app) -> None:
            await self.redis_queue.connect()
            asyncio.create_task(self.redis_worker.start())

        async def process(request):
            result = await proxy.process(await request.text())
            return web.json_response(text=result)

        app = web.Application()
        app.add_routes([web.post("/", process)])
        app.on_startup.append(startup)
        return app

    def test_stop(self) -> None:
        self.skipTest("Not working")
