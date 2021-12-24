import asyncio
import unittest

import fakeredis.aioredis as redis
from saq.job import Job
from saq.queue import Queue


class TestQueue(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.redis = redis.FakeRedis()
        self.queue = Queue(self.redis)
        self.queue._version = (6, 2, 0)

    async def test_enqueue(self):
        job = Job("test")
        await self.queue.enqueue(job)
        self.assertEqual(await self.queue.job(job.job_id), job)
        self.assertEqual(await self.queue.count("queued"), 1)
        await self.queue.enqueue(job)
        self.assertEqual(await self.queue.count("queued"), 1)
        await self.queue.enqueue(Job("test"))
        self.assertEqual(await self.queue.count("queued"), 2)
