"""Validate that the worker id in the context of the http proxy is the id of the worker rather than the queue."""

import unittest
from http.server import HTTPServer, BaseHTTPRequestHandler

from saq import Queue, Worker
from saq.queue.http import HttpProxy
from saq.types import Context
from tests.helpers import setup_postgres, create_postgres_queue, teardown_postgres
import asyncio
import threading


async def echo(_ctx: Context, *, a: int) -> int:
    return a


class ProxyRequestHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, proxy=None, **kwargs):
        self.proxy = proxy
        super().__init__(*args, **kwargs)

    def do_POST(self):
        length = int(self.headers["Content-Length"])
        body = self.rfile.read(length).decode("utf-8")
        response = asyncio.run(self.proxy.process(body))
        if response:
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(response.encode("utf-8"))
        else:
            self.send_response(200)
            self.end_headers()


class TestQueue(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        await setup_postgres()

    async def asyncTearDown(self) -> None:
        await teardown_postgres()

    async def test_http_proxy_with_two_workers(self) -> None:
        queue = await create_postgres_queue()
        proxy = HttpProxy(queue=queue)

        server = HTTPServer(
            ("localhost", 8080),
            lambda *args, **kwargs: ProxyRequestHandler(*args, proxy=proxy, **kwargs),
        )
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.daemon = True
        server_thread.start()

        queue1 = Queue.from_url("http://localhost:8080/")
        await queue1.connect()
        queue2 = Queue.from_url("http://localhost:8080/")
        await queue2.connect()

        worker = Worker(
            queue=queue1,
            functions=[echo],
        )
        await worker.stats()
        worker2 = Worker(
            queue=queue2,
            functions=[echo],
        )
        await worker2.stats()
        local_worker = Worker(
            queue=queue,
            functions=[echo],
        )
        await local_worker.stats()

        root_info = await queue.info()
        info1 = await queue1.info()
        info2 = await queue2.info()

        self.assertEqual(root_info["workers"], info1["workers"])
        self.assertEqual(info1["workers"], info2["workers"])
        self.assertEqual(info1["workers"].keys(), {worker.id, worker2.id, local_worker.id})
        self.assertEqual(info1["workers"].keys(), info2["workers"].keys())

        await queue1.disconnect()
        await queue2.disconnect()
        await queue.disconnect()

        server.shutdown()
        server_thread.join()
