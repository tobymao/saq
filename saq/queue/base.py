"""
Base Queue class
"""

from __future__ import annotations

import asyncio
import json
import logging
import typing as t
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager

from saq.job import (
    TERMINAL_STATUSES,
    UNSUCCESSFUL_TERMINAL_STATUSES,
    Job,
    Status,
    get_default_job_key,
)
from saq.utils import cancel_tasks, now, uuid1

if t.TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Sequence

    from saq.types import (
        BeforeEnqueueType,
        CountKind,
        ListenCallback,
        DumpType,
        LoadType,
        QueueInfo,
        QueueStats,
    )


logger = logging.getLogger("saq")


class JobError(Exception):
    """
    Basic Job error
    """

    def __init__(self, job: Job) -> None:
        super().__init__(
            f"Job {job.id} {job.status}\n\nThe above job failed with the following error:\n\n{job.error}"
        )
        self.job = job


class Queue(ABC):
    """An abstract base class for queues"""

    def __init__(
        self,
        name: str,
        dump: DumpType | None,
        load: LoadType | None,
    ) -> None:
        self.name = name
        self.uuid: str = uuid1()
        self.started: int = now()
        self.complete = 0
        self.failed = 0
        self.retried = 0
        self.aborted = 0
        self._dump = dump or json.dumps
        self._load = load or json.loads
        self._before_enqueues: dict[int, BeforeEnqueueType] = {}
        self.tasks: set[asyncio.Task[t.Any]] = set()

    def job_id(self, job_key: str) -> str:
        return job_key

    @abstractmethod
    async def disconnect(self) -> None:
        pass

    @abstractmethod
    async def info(
        self, jobs: bool = False, offset: int = 0, limit: int = 10
    ) -> QueueInfo:
        pass

    @abstractmethod
    async def count(self, kind: CountKind) -> int:
        pass

    @abstractmethod
    async def schedule(self, lock: int = 1) -> t.Any:
        pass

    @abstractmethod
    async def sweep(self, lock: int = 60, abort: float = 5.0) -> list[t.Any]:
        pass

    @abstractmethod
    async def listen(
        self,
        job_keys: Iterable[str],
        callback: ListenCallback,
        timeout: float | None = 10,
    ) -> None:
        pass

    @abstractmethod
    async def notify(self, job: Job) -> None:
        pass

    @abstractmethod
    async def update(self, job: Job) -> None:
        pass

    @abstractmethod
    async def job(self, job_key: str) -> Job | None:
        pass

    @abstractmethod
    async def abort(self, job: Job, error: str, ttl: float = 5) -> None:
        pass

    @abstractmethod
    async def dequeue(self, timeout: float = 0) -> Job | None:
        pass

    @abstractmethod
    async def get_abort_errors(self, jobs: Iterable[Job]) -> list[bytes | None]:
        pass

    @abstractmethod
    async def finish_abort(self, job: Job) -> None:
        pass

    @abstractmethod
    async def write_stats(self, stats: QueueStats, ttl: int) -> None:
        pass

    @abstractmethod
    async def _retry(self, job: Job, error: str | None) -> None:
        pass

    @abstractmethod
    async def _finish(
        self,
        job: Job,
        status: Status,
        *,
        result: t.Any = None,
        error: str | None = None,
    ) -> None:
        pass

    @abstractmethod
    async def _enqueue(self, job: Job) -> Job | None:
        pass

    @staticmethod
    def from_url(url: str, **kwargs: t.Any) -> Queue:
        """Create a queue with a Postgers or Redis url."""
        if url.startswith("redis"):
            from saq.queue.redis import RedisQueue

            return RedisQueue.from_url(url, **kwargs)

        if url.startswith("postgres"):
            from saq.queue.postgres import PostgresQueue

            return PostgresQueue.from_url(url, **kwargs)

        raise ValueError("URL is not valid")

    async def upkeep(self) -> None:
        """Start various upkeep tasks async."""

    async def stop(self) -> None:
        """Stop the queue and cleanup."""
        all_tasks = list(self.tasks)
        self.tasks.clear()
        await cancel_tasks(all_tasks)

    async def connect(self) -> None:
        pass

    def serialize(self, job: Job) -> bytes | str:
        return self._dump(job.to_dict())

    def deserialize(self, job_bytes: bytes | None) -> Job | None:
        if not job_bytes:
            return None

        job_dict = self._load(job_bytes)
        if job_dict.pop("queue") != self.name:
            raise ValueError(f"Job {job_dict} fetched by wrong queue: {self.name}")
        return Job(**job_dict, queue=self)

    async def stats(self, ttl: int = 60) -> QueueStats:
        stats: QueueStats = {
            "complete": self.complete,
            "failed": self.failed,
            "retried": self.retried,
            "aborted": self.aborted,
            "uptime": now() - self.started,
        }

        await self.write_stats(stats, ttl)
        return stats

    def register_before_enqueue(self, callback: BeforeEnqueueType) -> None:
        self._before_enqueues[id(callback)] = callback

    def unregister_before_enqueue(self, callback: BeforeEnqueueType) -> None:
        self._before_enqueues.pop(id(callback), None)

    async def retry(self, job: Job, error: str | None) -> None:
        job.status = Status.QUEUED
        job.error = error
        job.completed = 0
        job.started = 0
        job.progress = 0
        job.touched = now()

        await self._retry(job=job, error=error)

    async def finish(
        self,
        job: Job,
        status: Status,
        *,
        result: t.Any = None,
        error: str | None = None,
    ) -> None:
        job.status = status
        job.result = result
        job.error = error
        job.completed = now()

        if status == Status.COMPLETE:
            job.progress = 1.0

        await self._finish(job=job, status=status, result=result, error=error)

        if status == Status.COMPLETE:
            self.complete += 1
        elif status == Status.FAILED:
            self.failed += 1
        elif status == Status.ABORTED:
            self.aborted += 1

    async def enqueue(self, job_or_func: str | Job, **kwargs: t.Any) -> Job | None:
        """
        Enqueue a job by instance or string.

        Example:
            .. code-block::

                job = await queue.enqueue("add", a=1, b=2)
                print(job.id)

        Args:
            job_or_func: The job or function to enqueue.
                If a job instance is passed in, it's properties are overriden.
            kwargs: Kwargs can be arguments of the function or properties of the job.

        Returns:
            If the job has already been enqueued, this returns None, else Job
        """
        job_kwargs: dict[str, t.Any] = {}

        for k, v in kwargs.items():
            if k in Job.__dataclass_fields__:  # pylint: disable=no-member
                job_kwargs[k] = v
            else:
                job_kwargs.setdefault("kwargs", {})[k] = v

        if isinstance(job_or_func, str):
            job = Job(function=job_or_func, **job_kwargs)
        else:
            job = job_or_func

            for k, v in job_kwargs.items():
                setattr(job, k, v)

        if job.queue and job.queue.name != self.name:
            raise ValueError(f"Job {job} registered to a different queue")

        job.queue = self
        job.queued = now()
        job.status = Status.QUEUED

        await self._before_enqueue(job)

        return await self._enqueue(job)

    async def apply(
        self, job_or_func: str, timeout: float | None = None, **kwargs: t.Any
    ) -> t.Any:
        """
        Enqueue a job and wait for its result.

        If the job is successful, this returns its result.
        If the job is unsuccessful, this raises a JobError.

        Example:
            .. code-block::

                try:
                    assert await queue.apply("add", a=1, b=2) == 3
                except JobError:
                    print("job failed")

        Args:
            job_or_func: Same as Queue.enqueue
            timeout: If provided, how long to wait for result, else infinite (default None)
            kwargs: Same as Queue.enqueue
        """
        results = await self.map(job_or_func, timeout=timeout, iter_kwargs=[kwargs])
        if results:
            return results[0]
        return None

    async def map(
        self,
        job_or_func: str | Job,
        iter_kwargs: Sequence[dict[str, t.Any]],
        timeout: float | None = None,
        return_exceptions: bool = False,
        **kwargs: t.Any,
    ) -> list[t.Any]:
        """
        Enqueue multiple jobs and collect all of their results.

        Example:
            .. code-block::

                try:
                    assert await queue.map(
                        "add",
                        [
                            {"a": 1, "b": 2},
                            {"a": 3, "b": 4},
                        ]
                    ) == [3, 7]
                except JobError:
                    print("any of the jobs failed")

        Args:
            job_or_func: Same as Queue.enqueue
            iter_kwargs: Enqueue a job for each item in this sequence. Each item is the same
                as kwargs for Queue.enqueue.
            timeout: Total seconds to wait for all jobs to complete. If None (default) or 0, wait forever.
            return_exceptions: If False (default), an exception is immediately raised as soon as any jobs
                fail. Other jobs won't be cancelled and will continue to run.
                If True, exceptions are treated the same as successful results and aggregated in the result list.
            kwargs: Default kwargs for all jobs. These will be overridden by those in iter_kwargs.
        """
        iter_kwargs = [
            {
                "timeout": timeout,
                "key": kwargs.get("key", "") or get_default_job_key(),
                **kwargs,
                **kw,
            }
            for kw in iter_kwargs
        ]
        job_keys = [key["key"] for key in iter_kwargs]
        pending_job_keys = set(job_keys)

        async def callback(job_key: str, status: Status) -> bool:
            if status in TERMINAL_STATUSES:
                pending_job_keys.discard(job_key)

            if status in UNSUCCESSFUL_TERMINAL_STATUSES and not return_exceptions:
                return True

            if not pending_job_keys:
                return True

            return False

        # Start listening before we enqueue the jobs.
        # This ensures we don't miss any updates.
        task = asyncio.create_task(
            self.listen(pending_job_keys, callback, timeout=None)
        )

        try:
            await asyncio.gather(
                *[self.enqueue(job_or_func, **kw) for kw in iter_kwargs]
            )
        except Exception:
            task.cancel()
            raise

        await asyncio.wait_for(task, timeout=timeout)

        jobs: list[Job | None]
        jobs = await asyncio.gather(*[self.job(job_key) for job_key in job_keys])

        results: list[t.Any] = []
        for job in jobs:
            if job is None:
                continue
            if job.status in UNSUCCESSFUL_TERMINAL_STATUSES:
                exc = JobError(job)
                if not return_exceptions:
                    raise exc
                results.append(exc)
            else:
                results.append(job.result)
        return results

    @asynccontextmanager
    async def batch(self) -> AsyncIterator[None]:
        """
        Context manager to batch enqueue jobs.

        This tracks all jobs enqueued within the context manager scope and ensures that
        all are aborted if any exception is raised.

        Example:
            .. code-block::

                async with queue.batch():
                    await queue.enqueue("test")  # This will get cancelled
                    raise asyncio.CancelledError
        """
        children = set()

        async def track_child(job: Job) -> None:
            children.add(job)

        self.register_before_enqueue(track_child)

        try:
            yield
        except Exception:
            await asyncio.gather(
                *[self.abort(child, "cancelled") for child in children],
                return_exceptions=True,
            )
            raise
        finally:
            self.unregister_before_enqueue(track_child)

    async def _before_enqueue(self, job: Job) -> None:
        for cb in self._before_enqueues.values():
            await cb(job)
