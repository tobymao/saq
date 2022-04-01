import unittest

from saq import Worker
from saq.monitor import Monitor
from tests.helpers import create_queue, cleanup_queue


async def echo(_ctx, *, a):
    return a


functions = [echo]


class TestMonitor(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.queue1 = create_queue(name="queue1")
        self.queue2 = create_queue(name="queue2")
        self.worker1 = Worker(self.queue1, functions=functions)
        self.worker2 = Worker(self.queue2, functions=functions)
        self.monitor = Monitor(self.queue1)

    async def asyncTearDown(self):
        await cleanup_queue(self.queue1)
        await cleanup_queue(self.queue2)

    async def test_info(self):
        await self.queue1.enqueue("echo", a=1)
        await self.queue1.enqueue("echo", a=2)
        await self.queue2.enqueue("echo", a=3)
        await self.worker1.process()
        await self.worker2.process()
        info = await self.monitor.info(jobs=True)
        self.assertEqual(info, {})
        await self.queue1.stats()
        await self.queue2.stats()
        info = await self.monitor.info(jobs=True)
        self.assertIn("queue1", info)
        self.assertIn("queue2", info)
        self.assertEqual(info["queue1"]["queued"], 1)
        self.assertEqual(len(info["queue1"]["jobs"]), 1)
        self.assertEqual(info["queue1"]["jobs"][0]["kwargs"], repr({"a": 1}))
        self.assertEqual(info["queue2"]["queued"], 0)
        self.assertEqual(len(info["queue2"]["jobs"]), 0)

    async def test_job(self):
        job = await self.queue1.enqueue("echo", a=1)
        await self.worker1.process()
        monitor_job = await self.monitor.job(job.key)
        self.assertEqual(monitor_job.queue.name, "queue1")
        self.assertEqual(monitor_job.kwargs, {"a": 1})
        self.assertEqual(monitor_job.result, 1)
