"""
Postgres Queue
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
import typing as t
from contextlib import asynccontextmanager
from textwrap import dedent

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
    from psycopg import AsyncConnection, Notify
    from psycopg.sql import Identifier, SQL
    from psycopg_pool import AsyncConnectionPool
except ModuleNotFoundError as e:
    raise MissingDependencyError(
        "Missing dependencies for Postgres. Install them with `pip install saq[postgres]`."
    ) from e

SWEEP_LOCK_KEY = 0
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
        saq_lock_keyspace: The first of two advisory lock keys used by SAQ. (default 0)
            SAQ uses advisory locks for coordinating tasks between its workers, e.g. sweeping.
        job_lock_keyspace: The first of two advisory lock keys used for jobs. (default 1)
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
        saq_lock_keyspace: int = 0,
        job_lock_keyspace: int = 1,
    ) -> None:
        super().__init__(name=name, dump=dump, load=load)

        self.jobs_table = Identifier(jobs_table)
        self.stats_table = Identifier(stats_table)
        self.pool = pool
        self.min_size = min_size
        self.max_size = max_size
        self.poll_interval = poll_interval
        self.saq_lock_keyspace = saq_lock_keyspace
        self.job_lock_keyspace = job_lock_keyspace

        self.cond = asyncio.Condition()
        self.queue: asyncio.Queue = asyncio.Queue()
        self.waiting = 0  # Internal counter of worker tasks waiting for dequeue
        self.connection: AsyncConnection | None = None
        self.connection_lock = asyncio.Lock()
        self.released: list[str] = []
        self.has_sweep_lock = False
        self.tasks: list[asyncio.Task] = []

    async def connect(self) -> None:
        if self.connection:
            # If connection exists, connect() was already called
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

        self.tasks.append(asyncio.create_task(self.wait_for_job()))
        self.tasks.append(asyncio.create_task(self.listen_for_enqueues()))
        if self.poll_interval > 0:
            self.tasks.append(
                asyncio.create_task(self.dequeue_timer(self.poll_interval))
            )

    def job_id(self, job_key: str) -> str:
        return job_key

    def serialize(self, job: Job) -> bytes | str:
        """Ensure serialized job is in bytes because the job column is of type BYTEA."""
        serialized = self._dump(job.to_dict())
        if isinstance(serialized, str):
            return serialized.encode("utf-8")
        return serialized

    async def disconnect(self) -> None:
        if self.connection:
            await self.connection.cancel_safe()
            await self.pool.putconn(self.connection)
            self.connection = None
        await self.pool.close()
        for task in self.tasks:
            task.cancel()
        try:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        except asyncio.exceptions.CancelledError:
            pass
        self.has_sweep_lock = False

    async def info(
        self, jobs: bool = False, offset: int = 0, limit: int = 10
    ) -> QueueInfo:
        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                SQL(
                    dedent(
                        """
                        SELECT worker_id, stats FROM {stats_table}
                        WHERE %(now)s <= ttl
                        """
                    )
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
                        dedent(
                            """
                            SELECT job FROM {jobs_table}
                            WHERE status IN ('new', 'deferred', 'queued', 'active')
                            """
                        )
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

    async def count(self, kind: CountKind) -> int:
        async with self.pool.connection() as conn, conn.cursor() as cursor:
            if kind == "queued":
                await cursor.execute(
                    SQL(
                        dedent(
                            """
                            SELECT count(*) FROM {jobs_table}
                            WHERE status = 'queued'
                              AND queue = %(queue)s
                              AND %(now)s >= scheduled
                            """
                        )
                    ).format(jobs_table=self.jobs_table),
                    {"queue": self.name, "now": math.ceil(seconds(now()))},
                )
            elif kind == "active":
                await cursor.execute(
                    SQL(
                        dedent(
                            """
                            SELECT count(*) FROM {jobs_table}
                            WHERE status = 'active'
                              AND queue = %(queue)s
                            """
                        )
                    ).format(jobs_table=self.jobs_table),
                    {"queue": self.name},
                )
            elif kind == "incomplete":
                await cursor.execute(
                    SQL(
                        dedent(
                            """
                            SELECT count(*) FROM {jobs_table}
                            WHERE status IN ('new', 'deferred', 'queued', 'active')
                              AND queue = %(queue)s
                            """
                        )
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
        """Delete jobs and stats past their TTL and sweep stuck jobs"""
        swept = []

        if not self.has_sweep_lock:
            # Attempt to get the sweep lock and hold on to it
            async with self._get_connection() as conn, conn.cursor() as cursor, conn.transaction():
                await cursor.execute(
                    SQL("SELECT pg_try_advisory_lock({key1}, {key2})").format(
                        key1=self.saq_lock_keyspace,
                        key2=SWEEP_LOCK_KEY,
                    )
                )
                result = await cursor.fetchone()
            if result and not result[0]:
                # Could not acquire the sweep lock so another worker must already have it
                return []
            self.has_sweep_lock = True

        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                SQL(
                    dedent(
                        """
                -- Delete expired jobs
                DELETE FROM {jobs_table}
                WHERE status IN ('aborted', 'complete', 'failed')
                  AND {now} >= ttl;

                -- Delete expired stats
                DELETE FROM {stats_table}
                WHERE {now} >= ttl;

                -- Fetch active and aborting jobs without advisory locks
                WITH locks AS (
                  SELECT objid
                  FROM pg_locks
                  WHERE locktype = 'advisory'
                    AND classid = {job_lock_keyspace}
                    AND objsubid = 2 -- key is int pair, not single bigint
                )
                SELECT job
                FROM {jobs_table} LEFT OUTER JOIN locks ON lock_key = objid
                WHERE status IN ('active', 'aborting')
                  AND objid IS NULL;
                """
                    )
                ).format(
                    jobs_table=self.jobs_table,
                    stats_table=self.stats_table,
                    now=math.ceil(seconds(now())),
                    job_lock_keyspace=self.job_lock_keyspace,
                )
            )
            # Move cursor past result sets for the first two delete statements
            cursor.nextset()
            cursor.nextset()
            results = await cursor.fetchall()
            for result in results:
                job = self.deserialize(result[0])
                if not job:
                    continue
                swept.append(job.key.encode("utf-8"))
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

        async def _listen(gen: t.AsyncGenerator[Notify]) -> None:
            async for notify in gen:
                payload = json.loads(notify.payload)
                key = payload["key"]
                status = Status[payload["status"].upper()]
                if asyncio.iscoroutinefunction(callback):
                    stop = await callback(key, status)
                else:
                    stop = callback(key, status)

                if stop:
                    await gen.aclose()

        async with self.pool.connection() as conn:
            for key in job_keys:
                await conn.execute(SQL("LISTEN {}").format(Identifier(key)))
            await conn.commit()

            if timeout:
                await asyncio.wait_for(_listen(conn.notifies()), timeout)
            else:
                await _listen(conn.notifies())

    async def notify(self, job: Job, connection: AsyncConnection | None = None) -> None:
        payload = json.dumps({"key": job.key, "status": job.status})
        await self._notify(job.key, payload, connection)

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
                    dedent(
                        """
                        UPDATE {jobs_table} SET job = %(job)s, status = %(status)s, scheduled = %(scheduled)s, ttl = %(ttl)s
                        WHERE key = %(key)s
                        """
                    )
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
                    dedent(
                        """
                        SELECT job
                        FROM {jobs_table}
                        WHERE key = %(key)s
                        """
                    )
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

    async def _enqueue(self, job: Job) -> Job | None:
        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                SQL(
                    dedent(
                        """
                        INSERT INTO {jobs_table} (key, job, queue, status, scheduled)
                        VALUES (%(key)s, %(job)s, %(queue)s, %(status)s, %(scheduled)s)
                        ON CONFLICT (key) DO UPDATE
                        SET job = %(job)s, queue = %(queue)s, status = %(status)s, scheduled = %(scheduled)s, ttl = null
                        WHERE {jobs_table}.status IN ('aborted', 'complete', 'failed')
                          AND %(scheduled)s > {jobs_table}.scheduled
                        RETURNING job
                        """
                    )
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
                    dedent(
                        """
                        SELECT key, job
                        FROM {jobs_table}
                        WHERE key = ANY(%(keys)s)
                        """
                    )
                ).format(jobs_table=self.jobs_table),
                {"keys": [job.key for job in jobs]},
            )
            results: dict[str, bytes | None] = dict(await cursor.fetchmany())
        errors = []
        for job in jobs:
            result = self.deserialize(results.get(job.key))
            error = None
            if result:
                error = result.error
            errors.append(error.encode("utf-8") if error else None)
        return errors

    async def finish_abort(self, job: Job) -> None:
        await job.finish(Status.ABORTED, error=job.error)

    async def write_stats(self, stats: QueueStats, ttl: int) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                SQL(
                    dedent(
                        """
                        INSERT INTO {stats_table} (worker_id, stats, ttl)
                        VALUES (%(worker_id)s, %(stats)s, %(ttl)s)
                        ON CONFLICT (worker_id) DO UPDATE
                        SET stats = %(stats)s, ttl = %(ttl)s
                        """
                    )
                ).format(stats_table=self.stats_table),
                {
                    "worker_id": self.uuid,
                    "stats": json.dumps(stats),
                    "ttl": seconds(now()) + ttl,
                },
            )

    async def dequeue_timer(self, poll_interval: int) -> None:
        """Wakes up a single dequeue task every `poll_interval` seconds."""
        while True:
            async with self.cond:
                self.cond.notify(1)
            await asyncio.sleep(poll_interval)

    async def listen_for_enqueues(self, timeout: float | None = None) -> None:
        """Wakes up a single dequeue task when a Postgres enqueue notification is received."""
        async with self.pool.connection() as conn:
            await conn.execute(SQL("LISTEN {}").format(Identifier(ENQUEUE_CHANNEL)))
            await conn.commit()
            gen = conn.notifies(timeout=timeout)
            async for _ in gen:
                async with self.cond:
                    self.cond.notify(1)

    async def _retry(self, job: Job, error: str | None) -> None:
        next_retry_delay = job.next_retry_delay()
        if next_retry_delay:
            scheduled = time.time() + next_retry_delay
        else:
            scheduled = job.scheduled or seconds(now())

        await self.update(job, scheduled=int(scheduled))
        await self._release_job(job.key)

        self.retried += 1
        logger.info("Retrying %s", job.info(logger.isEnabledFor(logging.DEBUG)))

    async def _finish(
        self,
        job: Job,
        status: Status,
        *,
        result: t.Any = None,
        error: str | None = None,
    ) -> None:
        key = job.key

        async with self.pool.connection() as conn, conn.cursor() as cursor:
            if job.ttl >= 0:
                ttl = seconds(now()) + job.ttl if job.ttl > 0 else None
                await self.update(job, status=status, ttl=ttl, connection=conn)
            else:
                await cursor.execute(
                    SQL(
                        dedent(
                            """
                            DELETE FROM {jobs_table}
                            WHERE key = %(key)s
                            """
                        )
                    ).format(jobs_table=self.jobs_table),
                    {"key": key},
                )
                await self.notify(job, conn)
            await self._release_job(key)
            try:
                self.queue.task_done()
            except ValueError:
                # Error because task_done() called too many times, which happens in unit tests
                pass

            logger.info("Finished %s", job.info(logger.isEnabledFor(logging.DEBUG)))

    async def _dequeue(self) -> list[Job]:
        if not self.waiting:
            return []
        jobs = []
        async with self._get_connection() as conn, conn.cursor() as cursor, conn.transaction():
            await cursor.execute(
                SQL(
                    dedent(
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
                        WHERE {jobs_table}.key = locked_job.key
                          AND pg_try_advisory_lock({job_lock_keyspace}, locked_job.lock_key)
                        RETURNING job
                        """
                    )
                ).format(
                    jobs_table=self.jobs_table,
                    job_lock_keyspace=self.job_lock_keyspace,
                ),
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

    async def _notify(
        self, channel: str, payload: t.Any, connection: AsyncConnection | None = None
    ) -> None:
        async with (
            self.nullcontext(connection) if connection else self.pool.connection()
        ) as conn:
            await conn.execute(
                SQL("NOTIFY {channel}, {payload}").format(
                    channel=Identifier(channel), payload=payload
                )
            )

    @asynccontextmanager
    async def _get_connection(self) -> t.AsyncGenerator[AsyncConnection]:
        assert self.connection
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
        assert self.connection
        self.released.append(key)
        if self.connection_lock.locked():
            return
        async with self._get_connection() as conn:
            await conn.execute(
                SQL(
                    dedent(
                        """
                        SELECT pg_advisory_unlock({job_lock_keyspace}, lock_key)
                        FROM {jobs_table}
                        WHERE key = ANY(%(keys)s)
                        """
                    )
                ).format(
                    jobs_table=self.jobs_table,
                    job_lock_keyspace=self.job_lock_keyspace,
                ),
                {"keys": self.released},
            )
            await self.connection.commit()
        self.released.clear()
