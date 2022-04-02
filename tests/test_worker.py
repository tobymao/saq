import asyncio
import logging
import unittest
from unittest import mock

from saq.job import CronJob, Status
from saq.worker import Worker
from tests.helpers import create_queue, cleanup_queue


logging.getLogger().setLevel(logging.CRITICAL)


async def noop(_ctx):
    return 1


async def sleeper(ctx):
    await asyncio.sleep(ctx.get("sleep", 0.1))
    return {"a": 1, "b": []}


async def cron(_ctx):
    return 1


async def error(_ctx):
    raise ValueError("oops")


def sync_echo(_ctx, *, a):
    return a


functions = [noop, sleeper, error, sync_echo]


class TestWorker(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.queue = create_queue()
        self.worker = Worker(self.queue, functions=functions)

    async def asyncTearDown(self):
        await cleanup_queue(self.queue)

    async def test_start(self):
        task = asyncio.create_task(self.worker.start())
        job = await self.queue.enqueue("noop")
        await job.refresh(1)
        self.assertEqual(job.result, 1)
        job = await self.queue.enqueue("error")
        await asyncio.sleep(0.05)
        await job.refresh()
        self.assertEqual(job.status, Status.FAILED)
        assert "oops" in job.error
        job = await self.queue.enqueue("sleeper")
        self.assertEqual(job.status, Status.QUEUED)
        await asyncio.sleep(0.05)
        await job.refresh()
        self.assertEqual(job.status, Status.ACTIVE)
        task.cancel()
        await asyncio.sleep(0.05)
        await job.refresh()
        self.assertEqual(job.status, Status.QUEUED)

        asyncio.create_task(self.worker.start())
        job = await self.queue.enqueue("noop")
        await job.refresh(1)
        self.assertEqual(job.result, 1)
        job = await self.queue.enqueue("sleeper")
        self.assertEqual(job.status, Status.QUEUED)
        await asyncio.sleep(0.05)
        await job.refresh()
        self.assertEqual(job.status, Status.ACTIVE)
        await self.worker.stop()
        await job.refresh()
        self.assertEqual(job.status, Status.QUEUED)

    async def test_noop(self):
        job = await self.queue.enqueue("noop")
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

    async def test_sleeper(self):
        job = await self.queue.enqueue("sleeper")
        await self.worker.process()
        await job.refresh()
        assert job.queue != 0
        assert job.started != 0
        assert job.completed != 0
        self.assertEqual(job.status, Status.COMPLETE)
        self.assertEqual(job.result, {"a": 1, "b": []})

        job = await self.queue.enqueue("sleeper", timeout=0.05, retries=1)
        await self.worker.process()
        await self.worker.process()
        await job.refresh()
        self.assertEqual(job.attempts, 2)
        assert job.queue != 0
        assert job.started != 0
        assert job.completed != 0
        self.assertEqual(job.status, Status.FAILED)
        assert "TimeoutError" in job.error

    async def test_error(self):
        job = await self.queue.enqueue("error", retries=1)
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
        assert 'ValueError("oops")' in job.error

    def test_stop(self):
        loop = asyncio.new_event_loop()
        queue = create_queue()
        worker = Worker(queue, functions=functions)
        job = loop.run_until_complete(queue.enqueue("sleeper"))
        self.assertEqual(job.status, Status.QUEUED)
        worker.tasks.add(loop.create_task(worker.process()))

        loop.run_until_complete(asyncio.sleep(0.05))
        loop.run_until_complete(job.refresh())
        self.assertEqual(job.status, Status.ACTIVE)

        loop.run_until_complete(worker.stop())
        job = loop.run_until_complete(queue.job(job.id))
        loop.run_until_complete(cleanup_queue(queue))
        assert job.queued != 0
        assert job.started == 0
        assert job.completed == 0
        self.assertEqual(job.attempts, 1)
        self.assertEqual(job.status, Status.QUEUED)
        self.assertEqual(job.error, "cancelled")
        loop.close()

    async def test_hooks(self):
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

        await self.queue.enqueue("error", retries=0)
        await worker.process()
        self.assertEqual(x["before"], 2)
        self.assertEqual(x["after"], 2)

        task = asyncio.create_task(worker.process())
        await asyncio.sleep(0.05)
        task.cancel()
        self.assertEqual(x["before"], 2)
        self.assertEqual(x["after"], 2)

    @mock.patch("saq.utils.time")
    async def test_schedule(self, mock_time):
        mock_time.time.return_value = 1
        job = await self.queue.enqueue("noop", scheduled=2)
        self.assertEqual(await self.queue.count("queued"), 0)
        mock_time.time.return_value = 3
        await self.queue.schedule()
        await self.worker.process()
        await job.refresh()
        self.assertEqual(job.result, 1)

    @mock.patch("saq.worker.logger")
    @mock.patch("saq.utils.time")
    async def test_cron(self, mock_time, mock_logger):
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
        mock_logger.info.assert_any_call("Scheduled %s", [b"saq:job:cron:cron"])

    @mock.patch("saq.worker.logger")
    async def test_abort(self, mock_logger):
        job = await self.queue.enqueue("sleeper")
        self.worker.context["sleep"] = 60
        asyncio.create_task(self.worker.process())

        # wait for the job to actually start
        def callback(job_id, status):
            self.assertEqual(job.id, job_id)
            self.assertEqual(status, Status.ACTIVE)
            return True

        await self.queue.listen([job.id], callback)
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

    async def test_sync_function(self):
        asyncio.create_task(self.worker.start())
        assert await self.queue.apply("sync_echo", a=1) == 1
