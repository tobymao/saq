import logging
from aiohttp.test_utils import AioHTTPTestCase

from saq.job import Status
from saq.worker import Worker
from saq.web import create_app
from tests.helpers import create_queue, cleanup_queue


logging.getLogger().setLevel(logging.CRITICAL)


async def echo(_ctx, *, a):
    return a


functions = [echo]


class TestWorker(AioHTTPTestCase):
    async def get_application(self):
        self.queue1 = create_queue(name="queue1")
        self.queue2 = create_queue(name="queue2")
        self.worker = Worker(self.queue1, functions=functions)
        return create_app(queues=[self.queue1, self.queue2])

    async def asyncTearDown(self):
        await cleanup_queue(self.queue1)
        await cleanup_queue(self.queue2)

    async def test_queues(self):
        async with self.client.get("/api/queues") as resp:
            self.assertEqual(resp.status, 200)
            json = await resp.json()
            self.assertEqual(
                set(q["name"] for q in json["queues"]), {"queue1", "queue2"}
            )

        async with self.client.get(f"/api/queues/{self.queue1.name}") as resp:
            self.assertEqual(resp.status, 200)
            json = await resp.json()
            self.assertEqual(json["queue"]["name"], "queue1")

    async def test_jobs(self):
        job = await self.queue1.enqueue("echo", a=1)
        url = f"/api/queues/{self.queue1.name}/jobs/{job.key}"
        await self.worker.process()
        await job.refresh()
        self.assertEqual(job.status, Status.COMPLETE)

        async with self.client.get(url) as resp:
            self.assertEqual(resp.status, 200)
            json = await resp.json()
            self.assertEqual(json["job"]["kwargs"], repr({"a": 1}))
            self.assertEqual(json["job"]["result"], repr(1))

        async with self.client.post(f"{url}/retry") as resp:
            self.assertEqual(resp.status, 200)

        await job.refresh()
        self.assertEqual(job.status, Status.QUEUED)

        async with self.client.post(f"{url}/abort") as resp:
            self.assertEqual(resp.status, 200)

        await job.refresh()
        self.assertEqual(job.status, Status.ABORTED)
