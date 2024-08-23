"""
Postgres Queue
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
import typing as t
from contextlib import asynccontextmanager

from saq.errors import MissingDependencyError
from saq.job import (
    Job,
    Status,
)
from saq.queue.base import Queue, logger
from saq.queue.postgres_ddl import DDL_STATEMENTS
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
    from psycopg import AsyncConnection
    from psycopg.sql import Identifier, SQL
    from psycopg.types import json
    from psycopg_pool import AsyncConnectionPool
except ModuleNotFoundError as e:
    raise MissingDependencyError(
        "Missing dependencies for Postgres. Install them with `pip install saq[postgres]`."
    ) from e

SWEEP_LOCK_KEY = (1, 1)
ENQUEUE_CHANNEL = "saq:enqueue"
JOBS_TABLE = "saq_jobs"
STATS_TABLE = "saq_stats"


class PostgresQueue(Queue):
    """
    Queue is used to interact with Postgres.

    Args:
        pool: instance of psycopg_pool.AsyncConnectionPool
        name: name of the queue (default "default")
        jobs_table: name of the Postgres table SAQ will write jobs to (default "saq_jobs")
        stats_table: name of the Postgres table SAQ will write stats to (default "saq_stats")
        dump: lambda that takes a dictionary and outputs bytes (default `json.dumps`)
        load: lambda that takes str or bytes and outputs a python dictionary (default `json.loads`)
        min_size: minimum pool size. (default 4)
            The minimum number of Postgres connections.
        max_size: maximum pool size. (default 20)
            If greater than 0, this limits the maximum number of connections to Postgres.
            Otherwise, maintain `min_size` number of connections.
        poll_interval: how often to poll for jobs. (default 1)
            If 0, the queue will not poll for jobs and will only rely on notifications from the server.
            This mean cron jobs will not be picked up in a timely fashion.
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
        stats_table: str = STATS_TABLE,
        dump: DumpType | None = None,
        load: LoadType | None = None,
        min_size: int = 4,
        max_size: int = 20,
        poll_interval: int = 1,
    ) -> None:
        super().__init__(name=name, dump=dump, load=load)

        self.jobs_table = Identifier(jobs_table)
        self.stats_table = Identifier(stats_table)
        self.pool = pool
        self.min_size = min_size
        self.max_size = max_size
        self.poll_interval = poll_interval

        if dump:
            json.set_json_dumps(dump)
        if load:
            json.set_json_loads(load)

        self.cond = asyncio.Condition()
        self.queue: asyncio.Queue = asyncio.Queue()
        self.waiting = 0  # Internal counter of worker tasks waiting for dequeue
        self._connected = False
        self.connection_lock = asyncio.Lock()
        self.released: list[str] = []

    async def connect(self) -> None:
        if self._connected:
            return

        await self.pool.open()
        await self.pool.resize(min_size=self.min_size, max_size=self.max_size)
        async with self.pool.connection() as conn, conn.cursor() as cursor:
            for statement in DDL_STATEMENTS:
                await cursor.execute(
                    SQL(statement).format(
                        jobs_table=self.jobs_table, stats_table=self.stats_table
                    )
                )
        # Reserve a connection for dequeue and advisory locks
        self.connection = await self.pool.getconn()

        self.wait_for_job_task = asyncio.create_task(self.wait_for_job())
        self.listen_for_enqueues_task = asyncio.create_task(self.listen_for_enqueues())
        if self.poll_interval > 0:
            self.dequeue_timer_task = asyncio.create_task(
                self.dequeue_timer(self.poll_interval)
            )
        self._connected = True

    def job_id(self, job_key: str) -> str:
        return job_key

    def serialize(self, job: Job) -> json.Jsonb:
        return json.Jsonb(job.to_dict())

    def deserialize(self, job_dict: dict[t.Any, t.Any]) -> Job | None:
        if job_dict.pop("queue") != self.name:
            raise ValueError(f"Job {job_dict} fetched by wrong queue: {self.name}")
        return Job(**job_dict, queue=self)

    async def disconnect(self) -> None:
        await self.pool.putconn(self.connection)
        await self.pool.close()
        self.wait_for_job_task.cancel()
        self.listen_for_enqueues_task.cancel()
        if hasattr(self, "dequeue_timer_task"):
            self.dequeue_timer_task.cancel()

    async def info(
        self, jobs: bool = False, offset: int = 0, limit: int = 10
    ) -> QueueInfo:
        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                SQL(
                    """
                SELECT worker_id, stats FROM {stats_table}
                WHERE %(now)s <= ttl
                """
                ).format(stats_table=self.stats_table),
                {"now": seconds(now())},
            )
            results = await cursor.fetchall()
        workers: dict[str, dict[str, t.Any]] = dict(results)

        queued = await self.count("queued")
        active = await self.count("active")
        incomplete = await self.count("incomplete")

        if jobs:
            async with self.pool.connection() as conn, conn.cursor() as cursor:
                await cursor.execute(
                    SQL(
                        """
                    SELECT job FROM {jobs_table}
                    WHERE status IN ('new', 'deferred', 'queued', 'active')
                    """
                    ).format(jobs_table=self.jobs_table),
                )
                results = await cursor.fetchall()
            deserialized_jobs = (self.deserialize(result[0]) for result in results)
            jobs_info = [job.to_dict() for job in deserialized_jobs if job]
        else:
            jobs_info = []

        return {
            "workers": workers,
            "name": self.name,
            "queued": queued,
            "active": active,
            "scheduled": incomplete - queued - active,
            "jobs": jobs_info,
        }

    async def stats(self, ttl: int = 60) -> QueueStats:
        stats = self._get_stats()
        async with self.pool.connection() as conn:
            await conn.execute(
                SQL(
                    """
                INSERT INTO {stats_table} (worker_id, stats, ttl)
                VALUES (%(worker_id)s, %(stats)s, %(ttl)s)
                ON CONFLICT (worker_id) DO UPDATE
                SET stats = %(stats)s, ttl = %(ttl)s
                """
                ).format(stats_table=self.stats_table),
                {
                    "worker_id": self.uuid,
                    "stats": self._dump(stats),
                    "ttl": seconds(now()) + ttl,
                },
            )
        return stats

    async def count(self, kind: CountKind) -> int:
        async with self.pool.connection() as conn, conn.cursor() as cursor:
            if kind == "queued":
                await cursor.execute(
                    SQL(
                        """
                    SELECT count(*) FROM {jobs_table}
                    WHERE status = 'queued'
                      AND queue = %(queue)s
                      AND %(now)s >= scheduled
                    """
                    ).format(jobs_table=self.jobs_table),
                    {"queue": self.name, "now": math.ceil(seconds(now()))},
                )
            elif kind == "active":
                await cursor.execute(
                    SQL(
                        """
                    SELECT count(*) FROM {jobs_table}
                    WHERE status = 'active'
                      AND queue = %(queue)s
                    """
                    ).format(jobs_table=self.jobs_table),
                    {"queue": self.name},
                )
            elif kind == "incomplete":
                await cursor.execute(
                    SQL(
                        """
                    SELECT count(*) FROM {jobs_table}
                    WHERE status IN ('new', 'deferred', 'queued', 'active')
                      AND queue = %(queue)s
                    """
                    ).format(jobs_table=self.jobs_table),
                    {"queue": self.name},
                )
            else:
                raise ValueError("Can't count unknown type {kind}")

            result = await cursor.fetchone()
            assert result
            return result[0]

    async def schedule(self, lock: int = 1) -> t.Any: ...

    async def sweep(self, lock: int = 60, abort: float = 5.0) -> list[t.Any]:
        """Delete jobs past their TTL from the jobs table"""
        swept = []

        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                """
            SELECT pg_try_advisory_xact_lock(%(classid)s, %(objid)s)
            """,
                {"classid": SWEEP_LOCK_KEY[0], "objid": SWEEP_LOCK_KEY[1]},
            )
            result = await cursor.fetchone()
            if result and not result[0]:
                # Could not acquire the sweep lock so another worker must already be sweeping
                return []
            # Delete expired jobs
            await cursor.execute(
                SQL(
                    """
                DELETE FROM {jobs_table}
                WHERE status IN ('aborted', 'complete', 'failed')
                  AND %(now)s >= ttl
                """
                ).format(jobs_table=self.jobs_table),
                {"now": math.ceil(seconds(now()))},
            )
            # Delete expired stats
            await cursor.execute(
                SQL(
                    """
                DELETE FROM {stats_table}
                WHERE %(now)s >= ttl
                """
                ).format(stats_table=self.stats_table),
                {"now": math.ceil(seconds(now()))},
            )
            # Retry or mark orphaned jobs as aborted
            await cursor.execute(
                SQL(
                    """
                SELECT job
                FROM {jobs_table} LEFT OUTER JOIN pg_locks ON lock_key = objid
                WHERE status = 'active'
                  AND locktype = 'advisory'
                  AND classid = 0
                  AND objid IS NULL
                  AND objsubid = 2 -- key is int pair, not single bigint
                FOR UPDATE SKIP LOCKED
                """
                ).format(jobs_table=self.jobs_table)
            )
            results = await cursor.fetchall()
            for result in results:
                job = self.deserialize(result[0])
                if not job:
                    continue
                swept.append(job.key)
                if job.retryable:
                    await self.retry(job, error="swept")
                else:
                    await job.finish(Status.ABORTED, error="swept")
        return swept

    async def listen(
        self,
        job_keys: Iterable[str],
        callback: ListenCallback,
        timeout: float | None = 10,
    ) -> None:
        if not job_keys:
            return

        async def _listen() -> None:
            async with self.pool.connection() as conn:
                for key in job_keys:
                    await conn.execute(f'LISTEN "{key}"')
                await conn.commit()
                gen = conn.notifies()
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

        await asyncio.wait_for(_listen(), timeout or None)

    async def notify(self, job: Job, connection: AsyncConnection | None = None) -> None:
        payload = self._dump({"key": job.key, "status": job.status})
        await self._notify(job.key, payload, connection)

    async def _notify(
        self, channel: str, payload: t.Any, connection: AsyncConnection | None = None
    ) -> None:
        async with (
            self.nullcontext(connection) if connection else self.pool.connection()
        ) as conn:
            await conn.execute(f"NOTIFY \"{channel}\", '{payload}'")

    async def update(
        self,
        job: Job,
        status: Status | None = None,
        scheduled: int | None = None,
        ttl: float | None = None,
        connection: AsyncConnection | None = None,
    ) -> None:
        if status:
            job.status = status
        if scheduled:
            job.scheduled = scheduled
        job.touched = now()

        async with (
            self.nullcontext(connection) if connection else self.pool.connection()
        ) as conn:
            await conn.execute(
                SQL(
                    """
                    UPDATE {jobs_table} SET job = %(job)s, status = %(status)s, scheduled = %(scheduled)s, ttl = %(ttl)s
                    WHERE key = %(key)s
                    """
                ).format(jobs_table=self.jobs_table),
                {
                    "job": self.serialize(job),
                    "status": job.status,
                    "key": job.key,
                    "scheduled": job.scheduled,
                    "ttl": ttl,
                },
            )
            await self.notify(job, conn)

    async def job(self, job_key: str) -> Job | None:
        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                SQL(
                    """
                SELECT job
                FROM {jobs_table}
                WHERE key = %(key)s
                """
                ).format(jobs_table=self.jobs_table),
                {"key": job_key},
            )
            job = await cursor.fetchone()
            if job:
                return self.deserialize(job[0])
        return None

    async def abort(self, job: Job, error: str, ttl: float = 5) -> None:
        job.error = error
        job.status = Status.ABORTING
        job.ttl = int(seconds(now()) + ttl) + 1

        await self.update(job)

    async def retry(self, job: Job, error: str | None) -> None:
        self._update_job_for_retry(job, error)
        next_retry_delay = job.next_retry_delay()
        if next_retry_delay:
            scheduled = time.time() + next_retry_delay
        else:
            scheduled = job.scheduled or seconds(now())

        await self.update(job, scheduled=int(scheduled))
        await self._release_job(job.key)

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
                await self.update(
                    job, status=status, ttl=seconds(now()) + job.ttl, connection=conn
                )
            else:
                await cursor.execute(
                    SQL(
                        """
                    DELETE FROM {jobs_table}
                    WHERE key = %(key)s
                    """
                    ).format(jobs_table=self.jobs_table),
                    {"key": key},
                )
            await self._release_job(key)

            self._update_stats(status)

            await self.notify(job, conn)
            logger.info("Finished %s", job.info(logger.isEnabledFor(logging.DEBUG)))

    async def dequeue(self, timeout: float = 0) -> Job | None:
        """Wait on `self.cond` to dequeue.

        Retries indefinitely until a job is available or times out.
        """
        self.waiting += 1
        async with self.cond:
            self.cond.notify(1)

        try:
            return await asyncio.wait_for(self.queue.get(), timeout or None)
        except asyncio.exceptions.TimeoutError:
            return None
        finally:
            self.waiting -= 1

    async def wait_for_job(self) -> None:
        while True:
            async with self.cond:
                await self.cond.wait()

            for job in await self._dequeue():
                await self.queue.put(job)

    async def enqueue(self, job_or_func: str | Job, **kwargs: t.Any) -> Job | None:
        job = self._create_job_for_enqueue(job_or_func, **kwargs)

        await self._before_enqueue(job)

        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                SQL(
                    """
                INSERT INTO {jobs_table} (key, job, queue, status, scheduled)
                VALUES (%(key)s, %(job)s, %(queue)s, %(status)s, %(scheduled)s)
                ON CONFLICT (key) DO UPDATE
                SET job = %(job)s, queue = %(queue)s, status = %(status)s, scheduled = %(scheduled)s, ttl = null
                WHERE {jobs_table}.status IN ('aborted', 'complete', 'failed')
                RETURNING job
                """
                ).format(jobs_table=self.jobs_table),
                {
                    "key": job.key,
                    "job": self.serialize(job),
                    "queue": self.name,
                    "status": job.status,
                    "scheduled": job.scheduled or seconds(now()),
                },
            )

            if not await cursor.fetchone():
                return None

            await self._notify(ENQUEUE_CHANNEL, job.key, conn)

        logger.info("Enqueuing %s", job.info(logger.isEnabledFor(logging.DEBUG)))
        return job

    async def get_abort_errors(self, jobs: Iterable[Job]) -> list[bytes | None]:
        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                SQL(
                    """
                SELECT key, job->'error'
                FROM {jobs_table}
                WHERE key = ANY(%(keys)s)
                """
                ).format(jobs_table=self.jobs_table),
                {"keys": [job.key for job in jobs]},
            )
            results: dict[str, str | None] = dict(await cursor.fetchmany())
        errors = []
        for job in jobs:
            error = results.get(job.key)
            errors.append(error.encode("utf-8") if error else None)
        return errors

    async def finish_abort(self, job: Job) -> None:
        await job.finish(Status.ABORTED, error=job.error)

    async def dequeue_timer(self, poll_interval: int) -> None:
        """Wakes up a single dequeue task every `poll_interval` seconds."""
        while True:
            async with self.cond:
                self.cond.notify(1)
            await asyncio.sleep(poll_interval)

    async def listen_for_enqueues(self, timeout: float | None = None) -> None:
        """Wakes up a single dequeue task when a Postgres enqueue notification is received."""
        async with self.pool.connection() as conn:
            await conn.execute(f'LISTEN "{ENQUEUE_CHANNEL}"')
            await conn.commit()
            gen = conn.notifies(timeout=timeout)
            async for _ in gen:
                async with self.cond:
                    self.cond.notify(1)

    async def _dequeue(self) -> list[Job]:
        if not self.waiting:
            return []
        jobs = []
        async with self._get_connection() as conn, conn.cursor() as cursor, conn.transaction():
            await cursor.execute(
                SQL(
                    """
                WITH locked_job AS (
                  SELECT key, lock_key
                  FROM {jobs_table}
                  WHERE status = 'queued'
                    AND queue = %(queue)s
                    AND %(now)s >= scheduled
                  ORDER BY scheduled
                  LIMIT %(limit)s
                  FOR UPDATE SKIP LOCKED
                )
                UPDATE {jobs_table} SET status = 'active'
                FROM locked_job
                WHERE {jobs_table}.key = locked_job.key AND pg_try_advisory_lock(locked_job.lock_key)
                RETURNING job
                """
                ).format(jobs_table=self.jobs_table),
                {
                    "queue": self.name,
                    "now": math.ceil(seconds(now())),
                    "limit": self.waiting,
                },
            )
            results = await cursor.fetchall()
        for result in results:
            job = self.deserialize(result[0])
            if job:
                jobs.append(job)
        return jobs

    @asynccontextmanager
    async def _get_connection(self) -> t.AsyncGenerator[AsyncConnection]:
        async with self.connection_lock:
            yield self.connection

    @asynccontextmanager
    async def nullcontext(
        self, enter_result: t.Any | None = None
    ) -> t.AsyncGenerator[t.Any]:
        """Async version of contextlib.nullcontext

        Async support has been added to contextlib.nullcontext in Python 3.10.
        """
        yield enter_result

    async def _release_job(self, key: str) -> None:
        self.released.append(key)
        if self.connection_lock.locked():
            return
        async with self._get_connection() as conn:
            await conn.execute(
                SQL(
                    """
                SELECT pg_advisory_unlock(lock_key)
                FROM {jobs_table}
                WHERE key = ANY(%(keys)s)
                """
                ).format(jobs_table=self.jobs_table),
                {"keys": self.released},
            )
            await self.connection.commit()
        self.released.clear()
