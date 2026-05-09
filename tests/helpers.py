import asyncio
import typing as t

import psycopg

from saq.job import Job, Status
from saq.queue import Queue
from saq.queue.postgres import PostgresQueue
from saq.queue.redis import RedisQueue

POSTGRES_TEST_SCHEMA = "test_saq"


class StubQueue(Queue):
    """In-memory queue for testing base class and web endpoints without Redis/Postgres."""

    def __init__(self, name: str = "test") -> None:
        super().__init__(name=name, dump=None, load=None)
        self._jobs: dict[str, Job] = {}
        self._job_order: list[str] = []

    async def disconnect(self) -> None:
        pass

    async def info(self, jobs: bool = False, offset: int = 0, limit: int = 10) -> dict:
        all_jobs = list(self._jobs.values())
        return {
            "workers": {},
            "name": self.name,
            "queued": sum(1 for j in all_jobs if j.status == Status.QUEUED),
            "active": sum(1 for j in all_jobs if j.status == Status.ACTIVE),
            "scheduled": 0,
            "jobs": [j.to_dict() for j in all_jobs[offset:offset + limit]] if jobs else [],
        }

    async def count(self, kind: t.Any) -> int:
        if kind == "queued":
            return sum(1 for j in self._jobs.values() if j.status == Status.QUEUED)
        if kind == "active":
            return sum(1 for j in self._jobs.values() if j.status == Status.ACTIVE)
        return len(self._jobs)

    async def sweep(self, lock: int = 60, abort: float = 5.0) -> list[str]:
        return []

    async def _update(self, job: Job, status: Status | None = None, **kwargs: t.Any) -> None:
        self._jobs[job.key] = job

    async def job(self, job_key: str) -> Job | None:
        return self._jobs.get(job_key)

    async def jobs(self, job_keys: t.Any) -> list[Job | None]:
        return [self._jobs.get(k) for k in job_keys]

    async def iter_jobs(
        self,
        statuses: list[Status] | None = None,
        batch_size: int = 100,
        **kwargs: t.Any,
    ) -> t.Any:
        statuses_set = set(statuses or list(Status))
        function = kwargs.get("function")
        for key in self._job_order:
            job = self._jobs.get(key)
            if job and job.status in statuses_set:
                if function and job.function != function:
                    continue
                yield job

    async def abort(self, job: Job, error: str, ttl: float = 5) -> None:
        job.status = Status.ABORTING
        job.error = error
        self._jobs[job.key] = job

    async def dequeue(self, timeout: float = 0.0, poll_interval: float = 0.0) -> Job | None:
        return None

    async def write_worker_info(self, worker_id: str, info: t.Any, ttl: int) -> None:
        pass

    async def _retry(self, job: Job, error: str | None) -> None:
        self._jobs[job.key] = job

    async def _finish(self, job: Job, status: Status, **kwargs: t.Any) -> None:
        self._jobs[job.key] = job

    async def _enqueue(self, job: Job) -> Job | None:
        if job.key in self._jobs:
            return None
        self._jobs[job.key] = job
        self._job_order.append(job.key)
        return job


async def create_redis_queue(url="redis://localhost:6379", **kwargs: t.Any) -> RedisQueue:
    queue = t.cast(RedisQueue, Queue.from_url(url, **kwargs))
    await queue.connect()
    await queue.redis.flushdb()
    return queue


async def create_postgres_queue(**kwargs: t.Any) -> PostgresQueue:
    queue = t.cast(
        PostgresQueue,
        Queue.from_url(
            f"postgres://postgres@localhost?options=--search_path%3D{POSTGRES_TEST_SCHEMA}",
            **kwargs,
        ),
    )
    await queue.connect()
    await asyncio.sleep(0.1)  # Give some time for the tasks to start
    return queue


async def cleanup_queue(queue: Queue) -> None:
    if isinstance(queue, RedisQueue):
        await queue.redis.flushdb()
    await queue.disconnect()


async def setup_postgres() -> None:
    async with await psycopg.AsyncConnection.connect(
        "postgres://postgres@localhost", autocommit=True
    ) as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {POSTGRES_TEST_SCHEMA} CASCADE")
        await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {POSTGRES_TEST_SCHEMA}")


async def teardown_postgres() -> None:
    async with await psycopg.AsyncConnection.connect(
        "postgres://postgres@localhost", autocommit=True
    ) as conn:
        await conn.execute(f"DROP SCHEMA {POSTGRES_TEST_SCHEMA} CASCADE")
