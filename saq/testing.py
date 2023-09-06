"""
Testing helpers
"""
from __future__ import annotations

import typing as t
from collections.abc import Sequence
from unittest.mock import AsyncMock

from redis.asyncio.client import Redis

from saq.job import Job, Status
from saq.queue import Queue
from saq.utils import now
from saq.worker import Worker


class TestWorker(Worker):
    def __init__(self, settings_obj: dict[str, t.Any]) -> None:
        settings_obj["queue"] = AsyncMock(Queue)
        super().__init__(**settings_obj)


class TestQueue(Queue):
    """
    This is a test double of Queue, it's used in testing.
    Please refer to the Testing documentation for more detail.

    Args:
        worker: Optional worker instance
        settings: Optional settings dict

    Raises:
        AssertionError: If both `worker` and `settings` have been provided.
    """

    def __init__(
        self, *, worker: Worker | None = None, settings: dict[str, t.Any] | None = None
    ) -> None:
        super().__init__(redis=AsyncMock(spec=Redis))
        self._worker: Worker | None = None
        self._enqueued: list[Job] = []
        self._retried: list[Job] = []

        if worker and settings:
            raise AssertionError("Please provide worker or settings, not both")

        if worker:
            self._worker = worker
        elif settings:
            self._worker = TestWorker(settings)
        if self._worker:
            self._worker.queue = self

    async def update(self, job: Job) -> None:
        job.touched = now()

    async def abort(self, job: Job, error: str, ttl: float = 5) -> None:
        await job.finish(Status.ABORTED, error=error)

    async def _finish(self, job: Job, status: Status) -> None:
        return

    async def _retry(self, job: Job) -> None:
        self._retried.append(job)

    async def enqueue(self, job_or_func: str | Job, **kwargs: t.Any) -> Job:
        job = self.get_job(job_or_func, **kwargs)
        self._enqueued.append(job)
        return job

    async def map(
        self,
        job_or_func: str | Job,
        iter_kwargs: Sequence[dict[str, t.Any]],
        timeout: float | None = None,
        return_exceptions: bool = False,
        **kwargs: t.Any,
    ) -> list[t.Any]:
        if not self._worker:
            raise AssertionError(
                "Please pass in a settings object so a worker can be faked"
            )

        jobs: list[Job] = [
            self.get_job(job_or_func, timeout=timeout, **kwargs, **kw)
            for kw in iter_kwargs
        ]
        return await self._map(
            [self._worker.process_job(job) for job in jobs], return_exceptions
        )

    #########################################################################
    # Test assertion methods
    #########################################################################

    def getEnqueued(self, job_name: str, *, kwargs: dict | None = None) -> list[Job]:
        """
        Get jobs that have been enqueued, filtered by `job_name` and `kwargs`

        Args:
            job_name: Job name to filter by
            kwargs: Exact match of kwargs of job. (optional)
        """
        jobs = [job for job in self._enqueued if job.function == job_name]
        if kwargs:
            jobs = [job for job in jobs if job.kwargs == kwargs]
        return jobs

    def assertEnqueuedTimes(
        self, job_name: str, times: int, kwargs: dict | None = None
    ) -> None:
        """
        Asserts that the job(s) have been enqueued.

        Args:
            job_name: Job name to filter by
            times: Times the job was expected to be enqueued
            kwargs: Exact match of kwargs of job. (optional)

        Raises:
            AssertionError: If job has not been enqueued the right amount of times.
        """
        jobs = self.getEnqueued(job_name, kwargs=kwargs)

        if len(jobs) != times:
            raise AssertionError(
                f"Job '{job_name}' called {len(jobs)} times, expected {times}"
            )

    def assertNotEnqueued(self, job_name: str, *, kwargs: dict | None = None) -> None:
        """
        Asserts that the job(s) have **not** been enqueued.

        Args:
            job_name: Job name to filter by
            kwargs: Exact match of kwargs of job. (optional)

        Raises:
            AssertionError: If job has not been enqueued.
        """
        jobs = self.getEnqueued(job_name, kwargs=kwargs)

        if jobs:
            raise AssertionError(f"Job '{job_name}' called unexpectedly")

    def getRetried(self, job_name: str, *, kwargs: dict | None = None) -> list[Job]:
        """
        Get jobs that have been retried, filtered by `job_name` and `kwargs`

        Args:
            job_name: Job name to filter by
            kwargs: Exact match of kwargs of job. (optional)
        """
        jobs = [job for job in self._retried if job.function == job_name]
        if kwargs:
            jobs = [job for job in jobs if job.kwargs == kwargs]
        return jobs

    def assertRetriedTimes(
        self, job_name: str, times: int, kwargs: dict | None = None
    ) -> None:
        """
        Asserts that the job(s) have been retried.

        Args:
            job_name: Job name to filter by
            times: Times the job was expected to be retried
            kwargs: Exact match of kwargs of job. (optional)

        Raises:
            AssertionError: If job has not been retried the right amount of times.
        """
        jobs = self.getRetried(job_name, kwargs=kwargs)

        if len(jobs) != times:
            raise AssertionError(
                f"Job '{job_name}' retried {len(jobs)} times, expected {times}"
            )

    def assertNotRetried(self, job_name: str, *, kwargs: dict | None = None) -> None:
        """
        Asserts that the job(s) have **not** been retried..

        Args:
            job_name: Job name to filter by
            kwargs: Exact match of kwargs of job. (optional)

        Raises:
            AssertionError: If job has been retried.
        """
        jobs = self.getRetried(job_name, kwargs=kwargs)

        if jobs:
            raise AssertionError(f"Job '{job_name}' retried unexpectedly")
