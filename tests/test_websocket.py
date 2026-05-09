"""
Tests for WebSocket support in the aiohttp web UI.

Covers: WS connection/disconnection, broadcast, client management, and shutdown.
"""
from __future__ import annotations

import asyncio
import json
import unittest

from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, TestClient, TestServer

from saq.job import Status
from saq.web.aiohttp import QUEUES_KEY, WS_CLIENTS_KEY, create_app
from tests.helpers import StubQueue


class TestWebSocketConnect(AioHTTPTestCase):
    """Test WebSocket connection lifecycle."""

    async def get_application(self) -> web.Application:
        self.queue = StubQueue("ws_test")
        await self.queue.connect()
        return create_app([self.queue])

    async def test_ws_connect_and_receive_message(self):
        """Client connects and receives at least one broadcast message."""
        async with self.client.ws_connect("/ws") as ws:
            msg = await ws.receive(timeout=5)
            self.assertEqual(msg.type, web.WSMsgType.TEXT)
            data = json.loads(msg.data)
            self.assertIn("queues", data)
            self.assertIsInstance(data["queues"], list)

    async def test_ws_message_contains_queue_info(self):
        """Broadcast message contains queue name and stats."""
        async with self.client.ws_connect("/ws") as ws:
            msg = await ws.receive(timeout=5)
            data = json.loads(msg.data)
            self.assertEqual(len(data["queues"]), 1)
            q = data["queues"][0]
            self.assertEqual(q["name"], "ws_test")

    async def test_ws_client_registered_on_connect(self):
        """WS client is added to the clients set on connection."""
        async with self.client.ws_connect("/ws") as ws:
            clients = self.app[WS_CLIENTS_KEY]
            self.assertGreaterEqual(len(clients), 1)

    async def test_ws_client_removed_on_disconnect(self):
        """WS client is removed from the clients set after disconnect."""
        async with self.client.ws_connect("/ws") as ws:
            pass  # closes on exit
        await asyncio.sleep(0.1)
        clients = self.app[WS_CLIENTS_KEY]
        self.assertEqual(len(clients), 0)


class TestWebSocketMultipleClients(AioHTTPTestCase):
    """Test WebSocket with multiple simultaneous clients."""

    async def get_application(self) -> web.Application:
        self.queue = StubQueue("multi_test")
        await self.queue.connect()
        return create_app([self.queue])

    async def test_multiple_clients_receive_broadcast(self):
        """Multiple WS clients all receive broadcast messages."""
        async with self.client.ws_connect("/ws") as ws1, \
                   self.client.ws_connect("/ws") as ws2:
            msg1 = await ws1.receive(timeout=5)
            msg2 = await ws2.receive(timeout=5)
            self.assertEqual(msg1.type, web.WSMsgType.TEXT)
            self.assertEqual(msg2.type, web.WSMsgType.TEXT)
            data1 = json.loads(msg1.data)
            data2 = json.loads(msg2.data)
            self.assertIn("queues", data1)
            self.assertIn("queues", data2)


class TestWebSocketJobEndpoints(AioHTTPTestCase):
    """Test per-job retry/abort endpoints used by the inline buttons."""

    async def get_application(self) -> web.Application:
        self.queue = StubQueue("job_test")
        await self.queue.connect()
        return create_app([self.queue])

    async def test_retry_job_endpoint(self):
        """POST /api/queues/{queue}/jobs/{job}/retry retries a job."""
        job = await self.queue.enqueue("func_a")
        assert job
        await self.queue.finish(job, Status.FAILED, error="some error")
        resp = await self.client.post(
            f"/api/queues/{self.queue.name}/jobs/{job.key}/retry"
        )
        self.assertEqual(resp.status, 200)

    async def test_abort_job_endpoint(self):
        """POST /api/queues/{queue}/jobs/{job}/abort aborts a job."""
        job = await self.queue.enqueue("func_a")
        assert job
        resp = await self.client.post(
            f"/api/queues/{self.queue.name}/jobs/{job.key}/abort"
        )
        self.assertEqual(resp.status, 200)

    async def test_retry_nonexistent_job_returns_error(self):
        """POST retry for nonexistent job returns error."""
        resp = await self.client.post(
            f"/api/queues/{self.queue.name}/jobs/nonexistent/retry"
        )
        self.assertEqual(resp.status, 200)
        data = await resp.json()
        # Middleware catches exception and returns error JSON
        self.assertIn("error", data)

    async def test_abort_nonexistent_job_returns_error(self):
        """POST abort for nonexistent job returns error."""
        resp = await self.client.post(
            f"/api/queues/{self.queue.name}/jobs/nonexistent/abort"
        )
        self.assertEqual(resp.status, 200)
        data = await resp.json()
        self.assertIn("error", data)

    async def test_job_list_endpoint(self):
        """GET /api/queues/{queue}/jobs returns job list."""
        await self.queue.enqueue("func_a")
        resp = await self.client.get(f"/api/queues/{self.queue.name}/jobs")
        self.assertEqual(resp.status, 200)
        data = await resp.json()
        self.assertIn("jobs", data)

    async def test_job_list_filter_status(self):
        """GET /api/queues/{queue}/jobs?status=queued filters correctly."""
        job1 = await self.queue.enqueue("func_a")
        job2 = await self.queue.enqueue("func_b")
        assert job1 and job2
        await self.queue.finish(job1, Status.COMPLETE, result=1)
        resp = await self.client.get(
            f"/api/queues/{self.queue.name}/jobs", params={"status": "queued"}
        )
        self.assertEqual(resp.status, 200)
        data = await resp.json()
        self.assertEqual(len(data["jobs"]), 1)


class TestWebSocketBroadcastData(unittest.IsolatedAsyncioTestCase):
    """Test that broadcast sends correct data format."""

    async def asyncSetUp(self):
        self.queue = StubQueue("broadcast_test")
        await self.queue.connect()
        self.app = create_app([self.queue])
        self.server = TestServer(self.app)
        self.client = TestClient(self.server)
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()

    async def test_broadcast_data_structure(self):
        """Broadcast payload has 'queues' key with list of queue info dicts."""
        async with self.client.ws_connect("/ws") as ws:
            msg = await ws.receive(timeout=5)
            data = json.loads(msg.data)
            self.assertIn("queues", data)
            queues = data["queues"]
            self.assertIsInstance(queues, list)
            self.assertEqual(len(queues), 1)
            q = queues[0]
            self.assertEqual(q["name"], "broadcast_test")
            self.assertIn("queued", q)
            self.assertIn("active", q)
            self.assertIn("scheduled", q)
            self.assertIn("workers", q)

    async def test_broadcast_reflects_enqueue(self):
        """After enqueue, broadcast data reflects new queued count."""
        async with self.client.ws_connect("/ws") as ws:
            # Drain first message (before enqueue)
            await ws.receive(timeout=5)
            # Enqueue a job
            await self.queue.enqueue("func_a")
            # Wait for next broadcast
            msg = await ws.receive(timeout=5)
            data = json.loads(msg.data)
            q = data["queues"][0]
            self.assertEqual(q["queued"], 1)


if __name__ == "__main__":
    unittest.main()
