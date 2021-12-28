import asyncio
import logging
import unittest
from unittest import mock

from saq.job import Job, Status
from saq.queue import Queue
from saq.worker import Worker


logging.getLogger().setLevel(logging.CRITICAL)


async def noop(_ctx):
    return 1


async def sleep1(_ctx):
    asyncio.sleep(1)
    return 1


async def error(_ctx):
    raise ValueError("oops")


functions = [noop, error]


class TestWorker(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.queue = Queue.from_url("redis://localhost:6379")

        async def before_process(_ctx):
            return 1

        self.worker = Worker(self.queue, functions=functions)

    async def asyncTearDown(self):
        await self.queue.redis.flushdb()
        await self.queue.redis.close()

    async def test_start(self):
        pass

    async def test_noop(self):
        job = await self.queue.enqueue("noop")
        await self.worker.process()
        job = await self.queue.job(job.job_id)
        self.assertEqual(job.result, 1)

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
