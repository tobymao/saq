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
from saq.utils import now, seconds

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
        async with self.pool.connection() as conn, conn.cursor() as cursor:
            if kind == "queued":
                await cursor.execute(
                    f"""
                    SELECT count(*) FROM {self.jobs_table}
                    WHERE status = %(status)s AND %(now)s >= scheduled
                    """,
                    {"status": Status.QUEUED, "now": seconds(now())},
                )
            elif kind == "active":
                await cursor.execute(
                    f"""
                    SELECT count(*) FROM {JOBS_TABLE}
                    WHERE status = %(status)s
                    """,
                    {"status": Status.ACTIVE},
                )
            elif kind == "incomplete":
                await cursor.execute(
                    f"""
                    SELECT count(*) FROM {JOBS_TABLE}
                    WHERE status = ANY(%(statuses)s)
                    """,
                    {
                        "statuses": [
                            Status.NEW,
                            Status.DEFERRED,
                            Status.QUEUED,
                            Status.ACTIVE,
                        ]
                    },
                )
            else:
                raise ValueError("Can't count unknown type {kind}")

            result = await cursor.fetchone()
            assert result
            return result[0]

    async def schedule(self, lock: int = 1) -> t.Any: ...

    async def sweep(self, lock: int = 60, abort: float = 5.0) -> list[t.Any]:
        """Not applicable to Postgres queue"""
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
                    UPDATE {self.jobs_table} SET job = %(job)s, status = %(status)s
                    WHERE key = %(key)s
                    """,
                {"job": self.serialize(job), "status": job.status, "key": job.key},
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

        job.error = error
        job.status = Status.ABORTING

        await self.update(job)
        asyncio.create_task(self.delete(job, status=Status.ABORTED, delay=ttl))

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

        async with self.pool.connection() as conn, conn.cursor() as cursor:
            if job.ttl >= 0:
                await cursor.execute(
                    f"""
                    UPDATE {self.jobs_table} SET status = %(status)s, job = %(job)s
                    WHERE key = %(key)s
                    """,
                    {"status": status, "job": self.serialize(job), "key": key},
                )
                if job.ttl > 0:
                    asyncio.create_task(self.delete(job, delay=job.ttl))
            else:
                await cursor.execute(
                    f"""
                    DELETE FROM {self.jobs_table}
                    WHERE key = %(key)s
                    """,
                    {"key": key},
                )

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
                UPDATE {self.jobs_table} SET status = 'active'
                WHERE key = (
                  SELECT key
                  FROM {self.jobs_table}
                  WHERE status = 'queued' AND queue = %(queue)s AND %(now)s >= scheduled
                  FOR UPDATE SKIP LOCKED
                  LIMIT 1
                )
                RETURNING job
                """,
                {
                    "queue": self.name,
                    "now": seconds(now()),
                },
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
                INSERT INTO {self.jobs_table} (key, job, queue, status, scheduled)
                VALUES (%(key)s, %(job)s, %(queue)s, %(status)s, %(scheduled)s)
                ON CONFLICT (key) DO UPDATE
                SET job = %(job)s, queue = %(queue)s, status = %(status)s, scheduled = %(scheduled)s
                WHERE {self.jobs_table}.status NOT IN ('queued', 'aborting')
                RETURNING job
                """,
                {
                    "key": job.key,
                    "job": self.serialize(job),
                    "queue": self.name,
                    "status": job.status,
                    "scheduled": job.scheduled,
                },
            )

            if not await cursor.fetchone():
                return None

        logger.info("Enqueuing %s", job.info(logger.isEnabledFor(logging.DEBUG)))
        return job

    async def delete(
        self, job: Job, status: Status | None = None, delay: float = 0
    ) -> None:
        if delay:
            await asyncio.sleep(delay)

        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                (
                    f"""
                DELETE FROM {self.jobs_table}
                WHERE key = %(key)s
                """
                    + f"AND status = '{status}'"
                    if status
                    else ""
                ),
                {"key": job.key},
            )

    async def get_abort_errors(self, jobs: Iterable[Job]) -> list[bytes | None]:
        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                f"""
                SELECT key, job->'error'
                FROM {self.jobs_table}
                WHERE key = ANY(%(keys)s)
                """,
                {"keys": [job.key for job in jobs]},
            )
            results: dict[str, str | None] = dict(await cursor.fetchmany())
        errors = []
        for job in jobs:
            error = results.get(job.key)
            errors.append(error.encode("utf-8") if error else None)
        return errors

    async def finish_abort(self, job: Job) -> None:
        job.status = Status.ABORTED
        await self.update(job)
