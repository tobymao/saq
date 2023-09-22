from __future__ import annotations

import logging
import typing as t
import unittest

from aiohttp.test_utils import TestClient, TestServer

from saq.job import Status
from saq.web.aiohttp import create_app
from saq.worker import Worker
from tests.helpers import cleanup_queue, create_queue


if t.TYPE_CHECKING:
    from saq.types import Context, Function

logging.getLogger().setLevel(logging.CRITICAL)


async def echo(_ctx: Context, *, a: t.Any) -> t.Any:
    return a


functions: list[Function] = [echo]


class TestAiohttpWeb(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.queue1 = create_queue(name="queue1")
        self.queue2 = create_queue(name="queue2")
        self.worker = Worker(self.queue1, functions=functions)
        self.app = await self.get_application()
        self.client = await self.get_test_client()

    async def get_application(self) -> t.Any:
        server = TestServer(create_app(queues=[self.queue1, self.queue2]))
        await server.start_server()
        return server

    async def shutdown_application(self) -> None:
        await self.app.close()
        await self.client.close()

    async def get_test_client(self) -> t.Any:
        return TestClient(self.app)

    def status_code(self, resp: t.Any) -> int:
        return resp.status

    async def json(self, resp: t.Any) -> t.Any:
        return await resp.json()

    async def asyncTearDown(self) -> None:
        await cleanup_queue(self.queue1)
        await cleanup_queue(self.queue2)
        await self.shutdown_application()
        await super().asyncTearDown()

    async def test_queues(self) -> None:
        resp = await self.client.get("/api/queues")
        self.assertEqual(self.status_code(resp), 200)
        json = await self.json(resp)
        self.assertEqual({q["name"] for q in json["queues"]}, {"queue1", "queue2"})

        resp = await self.client.get(f"/api/queues/{self.queue1.name}")
        self.assertEqual(self.status_code(resp), 200)
        json = await self.json(resp)
        self.assertEqual(json["queue"]["name"], "queue1")

    async def test_jobs(self) -> None:
        job = await self.queue1.enqueue("echo", a=1)
        assert job is not None
        url = f"/api/queues/{self.queue1.name}/jobs/{job.key}"
        await self.worker.process()
        await job.refresh()
        self.assertEqual(job.status, Status.COMPLETE)

        resp = await self.client.get(url)
        self.assertEqual(self.status_code(resp), 200)
        json = await self.json(resp)
        self.assertEqual(json["job"]["kwargs"], repr({"a": 1}))
        self.assertEqual(json["job"]["result"], repr(1))

        resp = await self.client.post(f"{url}/retry")
        self.assertEqual(self.status_code(resp), 200)

        await job.refresh()
        self.assertEqual(job.status, Status.QUEUED)

        resp = await self.client.post(f"{url}/abort")
        self.assertEqual(self.status_code(resp), 200)

        await job.refresh()
        self.assertEqual(job.status, Status.ABORTED)
