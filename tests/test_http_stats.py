"""Validate that the worker id in the context of the http proxy is the id of the worker rather than the queue."""

import unittest

from saq import Worker
from saq.types import Context
from tests.helpers import (
    create_http_queue,
    create_postgres_queue,
    serve_http_proxy,
    setup_postgres,
    teardown_postgres,
)


async def echo(_ctx: Context, *, a: int) -> int:
    return a


class TestQueue(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await setup_postgres()
        self.queue = await create_postgres_queue()
        self.runner, self.url = await serve_http_proxy(self.queue)

    async def asyncTearDown(self) -> None:
        await teardown_postgres()
        await self.runner.cleanup()

    async def test_http_proxy_with_two_workers(self) -> None:
        queue1 = await create_http_queue(self.url)
        queue2 = await create_http_queue(self.url)

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
