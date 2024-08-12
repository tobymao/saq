"""
Postgres Queue
"""

from __future__ import annotations

import asyncio
import logging
import time
import typing as t

from saq.errors import MissingDependencyError
from saq.job import (
    Job,
    Status,
)
from saq.queue.base import Queue, logger
from saq.queue.postgres_ddl import CREATE_JOBS_TABLE
from saq.utils import now

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from saq.types import (
        CountKind,
        ListenCallback,
        DumpType,
        LoadType,
        QueueInfo,
        QueueStats,
    )

try:
    from psycopg.types import json
    from psycopg_pool import AsyncConnectionPool
except ModuleNotFoundError as e:
    raise MissingDependencyError(
        "Missing dependencies for Postgres. Install them with `pip install saq[postgres]`."
    ) from e

JOBS_TABLE = "saq_jobs"


class PostgresQueue(Queue):
    """
    Queue is used to interact with Postgres.

    Args:
        pool: instance of psycopg_pool.AsyncConnectionPool
        name: name of the queue (default "default")
        joabs_table: name of the Postgres table SAQ will write jobs to (default "saq_jobs")
        dump: lambda that takes a dictionary and outputs bytes (default `json.dumps`)
        load: lambda that takes str or bytes and outputs a python dictionary (default `json.loads`)
        min_size: minimum pool size. (default 4)
            The minimum number of Postgres connections.
        max_size: maximum pool size. (default 10)
            If greater than 0, this limits the maximum number of connections to Postgres.
            Otherwise, maintain `min_size` number of connections.
    """

    @classmethod
    def from_url(cls: type[PostgresQueue], url: str, **kwargs: t.Any) -> PostgresQueue:
        """Create a queue from a postgres url."""
        return cls(AsyncConnectionPool(url, open=False), **kwargs)

    def __init__(
        self,
        pool: AsyncConnectionPool,
        name: str = "default",
        jobs_table: str = JOBS_TABLE,
        dump: DumpType | None = None,
        load: LoadType | None = None,
        min_size: int = 4,
        max_size: int = 10,
    ) -> None:
        super().__init__(name=name, dump=dump, load=load)

        self.jobs_table = jobs_table
        self.pool = pool
        self.min_size = min_size
        self.max_size = max_size

        if dump:
            json.set_json_dumps(dump)
        if load:
            json.set_json_loads(load)

    async def connect(self) -> None:
        await self.pool.open()
        await self.pool.resize(min_size=self.min_size, max_size=self.max_size)
        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(CREATE_JOBS_TABLE.format(jobs_table=self.jobs_table))

    def job_id(self, job_key: str) -> str:
        return job_key

    def serialize(self, job: Job) -> json.Jsonb:
        return json.Jsonb(job.to_dict())

    def deserialize(self, job_dict: dict[t.Any, t.Any]) -> Job | None:
        if job_dict.pop("queue") != self.name:
            raise ValueError(f"Job {job_dict} fetched by wrong queue: {self.name}")
        return Job(**job_dict, queue=self)

    async def disconnect(self) -> None:
        await self.pool.close()

    async def info(
        self, jobs: bool = False, offset: int = 0, limit: int = 10
    ) -> QueueInfo:
        return {
            "workers": {},
            "name": self.name,
            "queued": 0,
            "active": 0,
            "scheduled": 0,
            "jobs": [],
        }

    async def stats(self, ttl: int = 60) -> QueueStats:
        return {
            "complete": self.complete,
            "failed": self.failed,
            "retried": self.retried,
            "aborted": self.aborted,
            "uptime": now() - self.started,
        }

    async def count(self, kind: CountKind) -> int:
        status = None
        if kind == "queued":
            status = [Status.QUEUED]
        if kind == "active":
            status = [Status.ACTIVE]
        if kind == "incomplete":
            status = [Status.NEW, Status.DEFERRED, Status.QUEUED, Status.ACTIVE]

        if status:
            async with self.pool.connection() as conn, conn.cursor() as cursor:
                await cursor.execute(
                    f"""
                    SELECT count(*) FROM {self.jobs_table}
                    WHERE status IN %(status)s
                    """,
                    {"status": status},
                )
                result = await cursor.fetchone()
                assert result
                return result[0]
        raise ValueError("Can't count unknown type {kind}")

    async def schedule(self, lock: int = 1) -> t.Any: ...

    async def sweep(self, lock: int = 60, abort: float = 5.0) -> list[t.Any]:
        return []

    async def listen(
        self,
        job_keys: Iterable[str],
        callback: ListenCallback,
        timeout: float | None = 10,
    ) -> None:
        async with self.pool.connection() as conn:
            await conn.set_autocommit(True)
            for key in job_keys:
                await conn.execute(f'LISTEN "{key}"')
            gen = conn.notifies(timeout=timeout or None)
            async for notify in gen:
                payload = self._load(notify.payload)
                key = payload["key"]
                status = Status[payload["status"].upper()]
                if asyncio.iscoroutinefunction(callback):
                    stop = await callback(key, status)
                else:
                    stop = callback(key, status)

                if stop:
                    await gen.aclose()

    async def notify(self, job: Job) -> None:
        async with self.pool.connection() as conn:
            payload = self._dump({"key": job.key, "status": job.status})
            await conn.execute(f"NOTIFY \"{job.key}\", '{payload}'")

    async def update(self, job: Job) -> None:
        job.touched = now()
        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"""
                    UPDATE {self.jobs_table} SET job = %(job)s
                    WHERE key = %(key)s
                    """,
                {"job": self.serialize(job), "key": job.key},
            )
        await self.notify(job)

    async def job(self, job_key: str) -> Job | None:
        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"""
                SELECT job FROM {self.jobs_table}
                WHERE key = %(key)s
                """,
                {"key": job_key},
            )
            job = await cursor.fetchone()
            if job:
                return self.deserialize(job[0])
        return None

    async def abort(self, job: Job, error: str, ttl: float = 5) -> None:
        await job.finish(Status.ABORTED, error=error)

    async def retry(self, job: Job, error: str | None) -> None:
        self._update_job_for_retry(job, error)
        next_retry_delay = job.next_retry_delay()
        if next_retry_delay:
            scheduled = time.time() + next_retry_delay
        else:
            scheduled = job.scheduled

        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"""
                UPDATE {self.jobs_table} SET status = %(status)s, job = %(job)s, scheduled = %(scheduled)s
                WHERE key = %(key)s
                """,
                {
                    "status": job.status,
                    "job": self.serialize(job),
                    "scheduled": scheduled,
                    "key": job.key,
                },
            )

            self.retried += 1
            await self.notify(job)
            logger.info("Retrying %s", job.info(logger.isEnabledFor(logging.DEBUG)))

    async def finish(
        self,
        job: Job,
        status: Status,
        *,
        result: t.Any = None,
        error: str | None = None,
    ) -> None:
        self._update_job_for_finish(job, status, result=result, error=error)
        key = job.key

        if job.ttl >= 0:
            async with self.pool.connection() as conn, conn.cursor() as cursor:
                await cursor.execute(
                    f"""
                    UPDATE {self.jobs_table} SET status = %(status)s, job = %(job)s
                    WHERE key = %(key)s
                    """,
                    {"status": status, "job": self.serialize(job), "key": key},
                )
        else:
            await self.delete(key)

        if status == Status.COMPLETE:
            self.complete += 1
        elif status == Status.FAILED:
            self.failed += 1
        elif status == Status.ABORTED:
            self.aborted += 1

        await self.notify(job)
        logger.info("Finished %s", job.info(logger.isEnabledFor(logging.DEBUG)))

    async def dequeue(self, timeout: float = 0) -> Job | None:
        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"""
                UPDATE {self.jobs_table} SET status = %(active)s
                WHERE key = (
                  SELECT key FROM {self.jobs_table}
                  WHERE status = %(queued)s AND queue = %(queue)s and EXTRACT(EPOCH FROM now()) >= scheduled
                  FOR UPDATE SKIP LOCKED
                  LIMIT 1
                )
                RETURNING job
                """,
                {"active": Status.ACTIVE, "queued": Status.QUEUED, "queue": self.name},
            )
            job = await cursor.fetchone()
            if job:
                return self.deserialize(job[0])
        return None

    async def enqueue(self, job_or_func: str | Job, **kwargs: t.Any) -> Job | None:
        job = self._create_job_for_enqueue(job_or_func, **kwargs)

        await self._before_enqueue(job)

        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"""
                INSERT INTO {self.jobs_table} (key, function, job, queue, status, scheduled)
                VALUES (%(key)s, %(function)s, %(job)s, %(queue)s, %(status)s, %(scheduled)s)
                ON CONFLICT (key) DO UPDATE
                SET function = %(function)s, job = %(job)s, queue = %(queue)s, status = %(status)s, scheduled = %(scheduled)s
                WHERE {self.jobs_table}.status != 'queued'
                """,
                {
                    "key": job.key,
                    "function": job.function,
                    "job": self.serialize(job),
                    "queue": self.name,
                    "status": job.status,
                    "scheduled": job.scheduled,
                },
            )

        logger.info("Enqueuing %s", job.info(logger.isEnabledFor(logging.DEBUG)))
        return job

    async def get_many(self, keys: Iterable[str]) -> list[bytes | None]:
        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"""
                SELECT job FROM {self.jobs_table}
                WHERE key in %(keys)s
                """,
                {"keys": keys},
            )
            return [row[0] for row in await cursor.fetchmany()]

    async def delete(self, key: str) -> None:
        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"""
                DELETE FROM {self.jobs_table}
                WHERE key = %(key)s
                """,
                {"key": key},
            )
