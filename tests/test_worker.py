import asyncio
import logging
import unittest
from unittest import mock

from saq.job import Status
from saq.worker import Worker
from tests.helpers import create_queue, cleanup_queue


logging.getLogger().setLevel(logging.CRITICAL)


async def noop(_ctx):
    return 1


async def sleeper(ctx):
    await asyncio.sleep(ctx.get("sleep", 0.1))
    return {"a": 1, "b": []}


async def error(_ctx):
    raise ValueError("oops")


functions = [noop, sleeper, error]


class TestWorker(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.queue = create_queue()
        self.worker = Worker(self.queue, functions=functions)

    async def asyncTearDown(self):
        await cleanup_queue(self.queue)

    async def test_start(self):
        pass

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

        job = await self.queue.enqueue("sleeper", timeout=0.05, retries=2)
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
        job = await self.queue.enqueue("error", retries=2)
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

    def test_handle_signal(self):
        loop = asyncio.new_event_loop()
        queue = create_queue()
        worker = Worker(queue, functions=functions)
        job = loop.run_until_complete(queue.enqueue("sleeper"))
        self.assertEqual(job.status, Status.QUEUED)
        loop.create_task(worker.process())
        loop.run_until_complete(asyncio.sleep(0.05))
        loop.run_until_complete(job.refresh())
        self.assertEqual(job.status, Status.ACTIVE)
        loop.run_until_complete(worker.handle_signal())
        assert not loop.is_running()

        loop = asyncio.new_event_loop()
        queue = create_queue()
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

        await self.queue.enqueue("error")
        await worker.process()
        self.assertEqual(x["before"], 2)
        self.assertEqual(x["after"], 1)

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

    async def test_abort(self):
        job = await self.queue.enqueue("sleeper")
        self.worker.context["sleep"] = 2
        asyncio.create_task(self.worker.process())

        def callback(job_id, status):
            self.assertEqual(job.id, job_id)
            self.assertEqual(status, Status.ACTIVE)
            return True

        await self.queue.listen(job, callback)
        await job.abort()
        await job.refresh()
        self.assertEqual(job.status, Status.ABORTED)
