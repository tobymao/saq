from __future__ import annotations

import logging
import typing as t
from aiohttp.test_utils import AioHTTPTestCase

from saq.job import Status
from saq.worker import Worker
from saq.web import create_app
from tests.helpers import create_queue, cleanup_queue

if t.TYPE_CHECKING:
    from aiohttp.web_app import Application

    from saq.types import Context, Function

logging.getLogger().setLevel(logging.CRITICAL)


async def echo(_ctx: Context, *, a: t.Any) -> t.Any:
    return a


functions: list[Function] = [echo]


class TestWorker(AioHTTPTestCase):
    async def get_application(self) -> Application:
        self.queue1 = create_queue(name="queue1")
        self.queue2 = create_queue(name="queue2")
        self.worker = Worker(self.queue1, functions=functions)
        return create_app(queues=[self.queue1, self.queue2])

    async def asyncTearDown(self) -> None:
        await cleanup_queue(self.queue1)
        await cleanup_queue(self.queue2)
        await super().asyncTearDown()

    async def test_queues(self) -> None:
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

    async def test_jobs(self) -> None:
        job = await self.queue1.enqueue("echo", a=1)
        assert job is not None
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
