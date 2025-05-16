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
from urllib.parse import urlparse

from saq.errors import InvalidUrlError
from saq.job import (
    TERMINAL_STATUSES,
    UNSUCCESSFUL_TERMINAL_STATUSES,
    Job,
    Status,
    get_default_job_key,
)
from saq.utils import now

if t.TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable, Sequence

    from saq.types import (
        BeforeEnqueueType,
        CountKind,
        ListenCallback,
        DumpType,
        LoadType,
        QueueInfo,
        WorkerStats,
        WorkerInfo,
    )


logger = logging.getLogger("saq")


DEFAULT_SWEPT_JOB_ERROR = "swept"
"""
The default error message to use when sweeping jobs.
"""


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
        swept_error_message: str | None = None,
    ) -> None:
        self.name = name
        self.started: int = now()
        self.complete = 0
        self.failed = 0
        self.retried = 0
        self.aborted = 0
        self._dump = dump or json.dumps
        self._load = load or json.loads
        self._swept_error_message = swept_error_message or DEFAULT_SWEPT_JOB_ERROR
        self._before_enqueues: dict[int, BeforeEnqueueType] = {}
        self._loop: asyncio.AbstractEventLoop | None = None

    def job_id(self, job_key: str) -> str:
        return job_key

    @property
    def swept_error_message(self) -> str:
        return self._swept_error_message

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop or asyncio.get_running_loop()

    @abstractmethod
    async def disconnect(self) -> None:
        pass

    @abstractmethod
    async def info(self, jobs: bool = False, offset: int = 0, limit: int = 10) -> QueueInfo:
        pass

    @abstractmethod
    async def count(self, kind: CountKind) -> int:
        pass

    async def schedule(self, _lock: int = 1) -> t.List[str]:
        return []

    @abstractmethod
    async def sweep(self, lock: int = 60, abort: float = 5.0) -> list[str]:
        pass

    async def notify(self, job: Job) -> None:
        pass

    async def update(self, job: Job, **kwargs: t.Any) -> None:
        job.touched = now()
        for k, v in kwargs.items():
            if hasattr(job, k):
                setattr(job, k, v)
        await self._update(self.copy(job), **kwargs)

    @abstractmethod
    async def _update(self, job: Job, status: Status | None = None, **kwargs: t.Any) -> None:
        pass

    @abstractmethod
    async def job(self, job_key: str) -> Job | None:
        pass

    @abstractmethod
    async def jobs(self, job_keys: t.Iterable[str]) -> t.List[Job | None]:
        pass

    @abstractmethod
    def iter_jobs(
        self,
        statuses: t.List[Status] = list(Status),
        batch_size: int = 100,
    ) -> t.AsyncIterator[Job]:
        pass

    @abstractmethod
    async def abort(self, job: Job, error: str, ttl: float = 5) -> None:
        pass

    @abstractmethod
    async def dequeue(self, timeout: float = 0) -> Job | None:
        pass

    async def finish_abort(self, job: Job) -> None:
        await job.finish(Status.ABORTED, error=job.error)

    @abstractmethod
    async def write_worker_info(
        self,
        worker_id: str,
        info: WorkerInfo,
        ttl: int,
    ) -> None:
        """
        Write stats and metadata for a worker.

        Args:
            worker_id: The worker id, passed in rather than taken from the queue instance to ensure that the stats
                are attributed to the worker and not the queue instance in the proxy server.
            queue_key: The key of the queue.
            metadata: The metadata to write.
            stats: The stats to write.
            ttl: The time-to-live in seconds.
        """
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
        """Create a queue with either a redis, postgres or http url."""
        parsed_url = urlparse(url)
        scheme = parsed_url.scheme.lower()
        if scheme.startswith("redis"):
            from saq.queue.redis import RedisQueue

            return RedisQueue.from_url(url, **kwargs)
        elif scheme.startswith("postgres"):
            from saq.queue.postgres import PostgresQueue

            return PostgresQueue.from_url(url, **kwargs)
        elif scheme.startswith("http"):
            from saq.queue.http import HttpQueue

            return HttpQueue.from_url(url, **kwargs)
        else:
            raise InvalidUrlError(f"Invalid url: {url}")

    async def connect(self) -> None:
        self._loop = asyncio.get_running_loop()

    def copy(self, job: Job) -> Job:
        return self.deserialize(job.to_dict())  # type: ignore

    def serialize(self, job: Job) -> bytes | str:
        return self._dump(job.to_dict())

    def deserialize(self, payload: dict | str | bytes | None) -> Job | None:
        if not payload:
            return None

        job_dict = payload if isinstance(payload, dict) else self._load(payload)
        if job_dict.pop("queue") != self.name:
            raise ValueError(f"Job {job_dict} fetched by wrong queue: {self.name}")
        return Job(**job_dict, queue=self)

    async def worker_info(
        self, worker_id: str, queue_key: str, metadata: t.Optional[dict] = None, ttl: int = 60
    ) -> WorkerInfo:
        """
        Method to be used by workers to update worker info.

        Args:
            worker_id: The worker id.
            ttl: Time stats are valid for in seconds.
            queue_key: The key of the queue.
            metadata: The metadata to write.

        Returns: Worker info.
        """
        stats: WorkerStats = {
            "complete": self.complete,
            "failed": self.failed,
            "retried": self.retried,
            "aborted": self.aborted,
            "uptime": now() - self.started,
        }
        info: WorkerInfo = {
            "stats": stats,
            "queue_key": queue_key,
            "metadata": metadata,
        }
        await self.write_worker_info(
            worker_id,
            info,
            ttl=ttl,
        )
        return info

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
        self.retried += 1
        logger.info("Retrying %s", job.info(logger.isEnabledFor(logging.DEBUG)))

    async def finish(
        self,
        job: Job,
        status: Status,
        *,
        result: t.Any = None,
        error: str | None = None,
        **kwargs: t.Any,
    ) -> None:
        job.status = status
        job.result = result
        job.error = error
        job.completed = now()

        if status == Status.COMPLETE:
            job.progress = 1.0

        await self._finish(job=job, status=status, result=result, error=error, **kwargs)
        logger.info("Finished %s", job.info(logger.isEnabledFor(logging.DEBUG)))

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
            if k in Job.__dataclass_fields__:
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

    async def listen(
        self,
        job_keys: Iterable[str],
        callback: ListenCallback,
        timeout: float | None = 10,
        poll_interval: float = 0.5,
    ) -> None:
        """
        Listen to updates on jobs.

        Args:
            job_keys: sequence of job keys
            callback: callback function, if it returns truthy, break
            timeout: if timeout is truthy, wait for timeout seconds
            poll_interval: number of seconds in between poll attempts if needed
        """

        async def listen() -> None:
            while True:
                for job in await self.jobs(job_keys):
                    if not job:
                        continue
                    if asyncio.iscoroutinefunction(callback):
                        stop = await callback(job.id, job.status)
                    else:
                        stop = callback(job.id, job.status)
                    if stop:
                        return
                await asyncio.sleep(poll_interval)

        if timeout:
            await asyncio.wait_for(listen(), timeout)
        else:
            await listen()

    async def apply(self, job_or_func: str, timeout: float | None = None, **kwargs: t.Any) -> t.Any:
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
        poll_interval: float = 0.5,
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
            poll_interval: number of seconds in between poll attempts
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

        await asyncio.gather(*(self.enqueue(job_or_func, **kw) for kw in iter_kwargs))
        incomplete = object()
        results = {key["key"]: incomplete for key in iter_kwargs}

        while remaining := [k for k, v in results.items() if v is incomplete]:
            for key, job in zip(remaining, await self.jobs(remaining)):
                if not job:
                    results[key] = None
                elif job.status in UNSUCCESSFUL_TERMINAL_STATUSES:
                    exc = JobError(job)
                    if not return_exceptions:
                        raise exc
                    results[key] = exc
                elif job.status in TERMINAL_STATUSES:
                    results[key] = job.result
            await asyncio.sleep(poll_interval)
        return list(results.values())

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
