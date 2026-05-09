"""
Tests for queue filtering, batch operations, and related Queue base class methods.

These tests use a mock/stub queue to test the base class logic without Redis/Postgres.
Tests that require real backends are in test_queue.py.
"""
from __future__ import annotations

import asyncio
import unittest
from typing import Any

import httpx

from saq.job import Job, Status
from saq.queue.base import Queue, JobError
from tests.helpers import StubQueue


class TestBatchRetry(unittest.IsolatedAsyncioTestCase):
    """Test Queue.batch_retry() base class method."""

    async def asyncSetUp(self):
        self.queue = StubQueue()
        await self.queue.connect()

    async def test_batch_retry_empty_list(self):
        """batch_retry with empty list returns zero counts."""
        result = await self.queue.batch_retry([])
        self.assertEqual(result, {"retried": 0, "failed": 0})

    async def test_batch_retry_nonexistent_keys(self):
        """batch_retry with nonexistent keys counts as failed."""
        result = await self.queue.batch_retry(["nonexistent"])
        self.assertEqual(result, {"retried": 0, "failed": 1})

    async def test_batch_retry_success(self):
        """batch_retry retries failed jobs."""
        job1 = await self.queue.enqueue("func_a")
        job2 = await self.queue.enqueue("func_b")
        assert job1 and job2
        await self.queue.finish(job1, Status.FAILED, error="err1")
        await self.queue.finish(job2, Status.FAILED, error="err2")
        result = await self.queue.batch_retry([job1.key, job2.key])
        self.assertEqual(result, {"retried": 2, "failed": 0})
        self.assertEqual((await self.queue.job(job1.key)).status, Status.QUEUED)
        self.assertEqual((await self.queue.job(job2.key)).status, Status.QUEUED)

    async def test_batch_retry_partial_failure(self):
        """batch_retry with mixed existent/nonexistent keys."""
        job1 = await self.queue.enqueue("func_a")
        assert job1
        await self.queue.finish(job1, Status.FAILED, error="err")
        result = await self.queue.batch_retry([job1.key, "nonexistent"])
        self.assertEqual(result, {"retried": 1, "failed": 1})


class TestBatchAbort(unittest.IsolatedAsyncioTestCase):
    """Test Queue.batch_abort() base class method."""

    async def asyncSetUp(self):
        self.queue = StubQueue()
        await self.queue.connect()

    async def test_batch_abort_empty_list(self):
        """batch_abort with empty list returns zero counts."""
        result = await self.queue.batch_abort([])
        self.assertEqual(result, {"aborted": 0, "failed": 0})

    async def test_batch_abort_nonexistent_keys(self):
        """batch_abort with nonexistent keys counts as failed."""
        result = await self.queue.batch_abort(["nonexistent"])
        self.assertEqual(result, {"aborted": 0, "failed": 1})

    async def test_batch_abort_success(self):
        """batch_abort aborts queued jobs."""
        job1 = await self.queue.enqueue("func_a")
        job2 = await self.queue.enqueue("func_b")
        assert job1 and job2
        result = await self.queue.batch_abort([job1.key, job2.key])
        self.assertEqual(result, {"aborted": 2, "failed": 0})

    async def test_batch_abort_partial_failure(self):
        """batch_abort with mixed keys."""
        job1 = await self.queue.enqueue("func_a")
        assert job1
        result = await self.queue.batch_abort([job1.key, "nonexistent"])
        self.assertEqual(result, {"aborted": 1, "failed": 1})


class TestJobListEndpoint(unittest.IsolatedAsyncioTestCase):
    """Test job listing with filtering on the Queue layer."""

    async def asyncSetUp(self):
        self.queue = StubQueue()
        await self.queue.connect()

    async def test_list_jobs_default(self):
        """list_jobs returns all jobs by default."""
        await self.queue.enqueue("func_a")
        await self.queue.enqueue("func_b")
        jobs = await self.queue.list_jobs()
        self.assertEqual(len(jobs), 2)

    async def test_list_jobs_filter_by_function(self):
        """list_jobs(function='func_a') returns only matching jobs."""
        await self.queue.enqueue("func_a")
        await self.queue.enqueue("func_b")
        jobs = await self.queue.list_jobs(function="func_a")
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].function, "func_a")

    async def test_list_jobs_filter_by_status(self):
        """list_jobs(statuses=[Status.QUEUED]) returns only queued jobs."""
        job1 = await self.queue.enqueue("func_a")
        job2 = await self.queue.enqueue("func_b")
        assert job1 and job2
        await self.queue.finish(job1, Status.COMPLETE, result=1)
        jobs = await self.queue.list_jobs(statuses=[Status.QUEUED])
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].function, "func_b")

    async def test_list_jobs_empty(self):
        """list_jobs on empty queue returns empty list."""
        jobs = await self.queue.list_jobs()
        self.assertEqual(len(jobs), 0)


class TestStarletteAPI(unittest.IsolatedAsyncioTestCase):
    """Test Starlette Web API endpoints with mock queue."""

    async def asyncSetUp(self):
        self.queue = StubQueue("testq")
        await self.queue.connect()
        from saq.web.starlette import saq_web

        self.app = saq_web("", queues=[self.queue])
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app), base_url="http://test"
        )

    async def asyncTearDown(self):
        await self.client.aclose()

    # --- Job listing API ---

    async def test_jobs_list_endpoint(self):
        """GET /api/queues/{queue}/jobs returns job list."""
        job = await self.queue.enqueue("func_a", a=1)
        assert job
        resp = await self.client.get(f"/api/queues/{self.queue.name}/jobs")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("jobs", data)
        self.assertEqual(len(data["jobs"]), 1)

    async def test_jobs_list_filter_status(self):
        """GET /api/queues/{queue}/jobs?status=queued filters by status."""
        job1 = await self.queue.enqueue("func_a")
        job2 = await self.queue.enqueue("func_b")
        assert job1 and job2
        await self.queue.finish(job1, Status.COMPLETE, result=1)
        resp = await self.client.get(
            f"/api/queues/{self.queue.name}/jobs", params={"status": "queued"}
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(len(data["jobs"]), 1)
        self.assertEqual(data["jobs"][0]["function"], "func_b")

    async def test_jobs_list_filter_function(self):
        """GET /api/queues/{queue}/jobs?function=func_a filters by function."""
        await self.queue.enqueue("func_a")
        await self.queue.enqueue("func_b")
        resp = await self.client.get(
            f"/api/queues/{self.queue.name}/jobs", params={"function": "func_a"}
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(len(data["jobs"]), 1)
        self.assertEqual(data["jobs"][0]["function"], "func_a")

    async def test_jobs_list_empty(self):
        """GET /api/queues/{queue}/jobs on empty queue returns empty list."""
        resp = await self.client.get(f"/api/queues/{self.queue.name}/jobs")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(len(data["jobs"]), 0)

    # --- Batch retry API ---

    async def test_batch_retry_endpoint(self):
        """POST /api/queues/{queue}/jobs/batch/retry retries jobs."""
        job1 = await self.queue.enqueue("func_a")
        job2 = await self.queue.enqueue("func_b")
        assert job1 and job2
        await self.queue.finish(job1, Status.FAILED, error="err")
        await self.queue.finish(job2, Status.FAILED, error="err")
        resp = await self.client.post(
            f"/api/queues/{self.queue.name}/jobs/batch/retry",
            json={"keys": [job1.key, job2.key]},
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["retried"], 2)
        self.assertEqual(data["failed"], 0)

    async def test_batch_retry_no_keys(self):
        """POST batch/retry without keys returns 400."""
        resp = await self.client.post(
            f"/api/queues/{self.queue.name}/jobs/batch/retry",
            json={},
        )
        self.assertEqual(resp.status_code, 400)

    # --- Batch abort API ---

    async def test_batch_abort_endpoint(self):
        """POST /api/queues/{queue}/jobs/batch/abort aborts jobs."""
        job1 = await self.queue.enqueue("func_a")
        assert job1
        resp = await self.client.post(
            f"/api/queues/{self.queue.name}/jobs/batch/abort",
            json={"keys": [job1.key]},
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(data["aborted"], 1)
        self.assertEqual(data["failed"], 0)

    async def test_batch_abort_no_keys(self):
        """POST batch/abort without keys returns 400."""
        resp = await self.client.post(
            f"/api/queues/{self.queue.name}/jobs/batch/abort",
            json={},
        )
        self.assertEqual(resp.status_code, 400)

    # --- Existing endpoints still work ---

    async def test_queues_endpoint(self):
        """GET /api/queues still works after adding new endpoints."""
        resp = await self.client.get("/api/queues")
        self.assertEqual(resp.status_code, 200)

    async def test_health_endpoint(self):
        """GET /health still works."""
        resp = await self.client.get("/health")
        self.assertEqual(resp.status_code, 200)


if __name__ == "__main__":
    unittest.main()
