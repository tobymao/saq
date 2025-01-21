"""Validate that the worker id in the context of the http proxy is the id of the worker rather than the queue."""

import unittest
from aiohttp import web

from saq import Queue, Worker
from saq.queue.http import HttpProxy
from saq.types import Context
from tests.helpers import setup_postgres, create_postgres_queue, teardown_postgres


async def echo(_ctx: Context, *, a: int) -> int:
    return a


class TestQueue(unittest.IsolatedAsyncioTestCase):
    async def handle_post(self, request):
        body = await request.text()
        response = await self.proxy.process(body)
        if response:
            return web.Response(text=response, content_type="application/json")
        else:
            return web.Response(status=200)

    async def asyncSetUp(self) -> None:
        await setup_postgres()
        self.queue = await create_postgres_queue()
        self.proxy = HttpProxy(queue=self.queue)
        self.app = web.Application()
        self.app.add_routes([web.post("/", self.handle_post)])
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "localhost", 8080)
        await self.site.start()

    async def asyncTearDown(self) -> None:
        await teardown_postgres()
        await self.site.stop()
        await self.runner.cleanup()

    async def test_http_proxy_with_two_workers(self) -> None:
        queue1 = Queue.from_url("http://localhost:8080/")
        await queue1.connect()
        queue2 = Queue.from_url("http://localhost:8080/")
        await queue2.connect()

        worker = Worker(
            queue=queue1,
            functions=[echo],
        )
        await worker.worker_info()
        worker2 = Worker(
            queue=queue2,
            functions=[echo],
        )
        await worker2.worker_info()
        local_worker = Worker(
            queue=self.queue,
            functions=[echo],
        )
        await local_worker.worker_info()

        root_info = await self.queue.info()
        info1 = await queue1.info()
        info2 = await queue2.info()

        self.assertEqual(root_info["workers"], info1["workers"])
        self.assertEqual(info1["workers"], info2["workers"])
        self.assertEqual(info1["workers"].keys(), {worker.id, worker2.id, local_worker.id})
        self.assertEqual(info1["workers"].keys(), info2["workers"].keys())

        await queue1.disconnect()
        await queue2.disconnect()
        await self.queue.disconnect()
