"""
Postgres Queue using asyncpg
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
    from asyncpg.pool import PoolConnectionProxy
    from saq.types import (
        CountKind,
        ListenCallback,
        DumpType,
        LoadType,
        QueueInfo,
        QueueStats,
    )

try:
    import asyncpg
    from asyncpg import Connection, Pool
except ModuleNotFoundError as e:
    raise MissingDependencyError(
        "Missing dependencies for Postgres. Install them with `pip install saq[asyncpg]`."
    ) from e

ENQUEUE_CHANNEL = "saq:enqueue"
JOBS_TABLE = "saq_jobs"
STATS_TABLE = "saq_stats"


class PostgresQueue(Queue):
    """
    Queue is used to interact with Postgres using asyncpg.

    Args:
        pool: instance of asyncpg.Pool
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
        pool = asyncpg.create_pool(dsn=url, **kwargs)
        return cls(pool, **kwargs)

    def __init__(
        self,
        pool: Pool,
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

        self.jobs_table = jobs_table
        self.stats_table = stats_table
        self.pool = pool
        self.min_size = min_size
        self.max_size = max_size
        self.poll_interval = poll_interval
        self.saq_lock_keyspace = saq_lock_keyspace
        self.job_lock_keyspace = job_lock_keyspace

        self.cond = asyncio.Condition()
        self.queue: asyncio.Queue = asyncio.Queue()
        self.waiting = 0  # Internal counter of worker tasks waiting for dequeue
        self.connection: PoolConnectionProxy | None = None
        self.connection_lock = asyncio.Lock()
        self.released: list[str] = []
        self.has_sweep_lock = False

    async def init_db(self) -> None:
        async with self.pool.acquire() as conn:
            for statement in DDL_STATEMENTS:
                await conn.execute(statement.format(jobs_table=self.jobs_table, stats_table=self.stats_table))

    async def upkeep(self) -> None:
        await self.init_db()

        self.tasks.add(asyncio.create_task(self.wait_for_job()))
        self.tasks.add(asyncio.create_task(self.listen_for_enqueues()))
        if self.poll_interval > 0:
            self.tasks.add(asyncio.create_task(self.dequeue_timer(self.poll_interval)))

    async def connect(self) -> None:
        await self.pool # ensure the pool is created sync `from_url` does not await the `create_pool` return`
        if self.connection:
            # If connection exists, connect() was already called
            return

        # Reserve a connection for dequeue and advisory locks
        self.connection = await self.pool.acquire()

    def serialize(self, job: Job) -> bytes | str:
        """Ensure serialized job is in bytes because the job column is of type BYTEA."""
        serialized = self._dump(job.to_dict())
        if isinstance(serialized, str):
            return serialized.encode("utf-8")
        return serialized

    async def disconnect(self) -> None:
        async with self.connection_lock:
            if self.connection:
                await self.pool.release(self.connection)
                self.connection = None
        await self.pool.close()
        self.has_sweep_lock = False

    async def info(self, jobs: bool = False, offset: int = 0, limit: int = 10) -> QueueInfo:
        async with self.pool.acquire() as conn:
            results = await conn.fetch(
                f"""
                SELECT worker_id, stats FROM {self.stats_table}
                WHERE $1 <= expire_at
                """,
                seconds(now()),
            )
        workers: dict[str, dict[str, t.Any]] = {row['worker_id']: json.loads(row['stats']) for row in results}

        queued = await self.count("queued")
        active = await self.count("active")
        incomplete = await self.count("incomplete")

        if jobs:
            async with self.pool.acquire() as conn:
                results = await conn.fetch(
                    f"""
                    SELECT job FROM {self.jobs_table}
                    WHERE status IN ('new', 'deferred', 'queued', 'active')
                    """
                )
            deserialized_jobs = (self.deserialize(result['job']) for result in results)
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
        async with self.pool.acquire() as conn:
            if kind == "queued":
                result = await conn.fetchval(
                    f"""
                    SELECT count(*) FROM {self.jobs_table}
                    WHERE status = 'queued'
                      AND queue = $1
                      AND $2 >= scheduled
                    """,
                    self.name,
                    math.ceil(seconds(now())),
                )
            elif kind == "active":
                result = await conn.fetchval(
                    f"""
                    SELECT count(*) FROM {self.jobs_table}
                    WHERE status = 'active'
                      AND queue = $1
                    """,
                    self.name,
                )
            elif kind == "incomplete":
                result = await conn.fetchval(
                    f"""
                    SELECT count(*) FROM {self.jobs_table}
                    WHERE status IN ('new', 'deferred', 'queued', 'active')
                      AND queue = $1
                    """,
                    self.name,
                )
            else:
                raise ValueError(f"Can't count unknown type {kind}")

            return result

    async def sweep(self, lock: int = 60, abort: float = 5.0) -> list[str]:
        """Delete jobs and stats past their expiration and sweep stuck jobs"""
        swept = []

        if not self.has_sweep_lock:
            # Attempt to get the sweep lock and hold on to it
            async with self._get_connection() as conn:
                result = await conn.fetchval(
                    "SELECT pg_try_advisory_lock($1, hashtext($2))",
                    self.saq_lock_keyspace,
                    self.name,
                )
            if not result:
                # Could not acquire the sweep lock so another worker must already have it
                return []
            self.has_sweep_lock = True

        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                -- Delete expired jobs
                DELETE FROM {self.jobs_table}
                WHERE queue = $1
                  AND status IN ('aborted', 'complete', 'failed')
                  AND $2 >= expire_at;
                """,
                self.name,
                math.ceil(seconds(now())),
            )

            await conn.execute(
                f"""
                -- Delete expired stats
                DELETE FROM {self.stats_table}
                WHERE $1 >= expire_at;
                """,
                math.ceil(seconds(now())),
            )

            results = await conn.fetch(
                f"""
                -- Fetch active and aborting jobs without advisory locks
                WITH locks AS (
                  SELECT objid
                  FROM pg_locks
                  WHERE locktype = 'advisory'
                    AND classid = $1
                    AND objsubid = 2 -- key is int pair, not single bigint
                )
                SELECT key, job, objid
                FROM {self.jobs_table} LEFT OUTER JOIN locks ON lock_key = objid
                WHERE queue = $2
                  AND status IN ('active', 'aborting');
                """,
                self.job_lock_keyspace,
                self.name,
            )
            for row in results:
                key, job_bytes, objid = row['key'], row['job'], row['objid']
                job = self.deserialize(job_bytes)
                assert job
                if objid and not job.stuck:
                    continue

                swept.append(key)
                await self.abort(job, error="swept")

                try:
                    await job.refresh(abort)
                except asyncio.TimeoutError:
                    logger.info("Could not abort job %s", key)

                logger.info("Sweeping job %s", job.info(logger.isEnabledFor(logging.DEBUG)))
                if job.retryable:
                    await self.retry(job, error="swept")
                else:
                    await self.finish(job, Status.ABORTED, error="swept")
        return swept

    async def listen(
        self,
        job_keys: Iterable[str],
        callback: ListenCallback,
        timeout: float | None = 10,
    ) -> None:
        if not job_keys:
            return

        async def _listen(conn: PoolConnectionProxy | Connection, pid: int, channel: str, payload: str) -> None:
            payload_data = json.loads(payload)
            key = payload_data["key"]
            status = Status[payload_data["status"].upper()]
            if asyncio.iscoroutinefunction(callback):
                _ = await callback(key, status)
            else:
                _ = callback(key, status)
 
        async with self.pool.acquire() as conn:
            for key in job_keys:
                await conn.add_listener(key, _listen) 

            if timeout:
                try:
                    await asyncio.sleep(timeout)
                finally:
                    for key in job_keys:
                        await conn.remove_listener(key, _listen)
            else:
                # If no timeout, we'll keep listening indefinitely
                while True:
                    await asyncio.sleep(30)  # Sleep for 30 seconds and then check again

    async def notify(self, job: Job, connection: PoolConnectionProxy | None = None) -> None:
        payload = json.dumps({"key": job.key, "status": job.status})
        await self._notify(job.key, payload, connection)

    async def update(
        self,
        job: Job,
        connection: PoolConnectionProxy | None = None,
        expire_at: float | None = -1,
        **kwargs: t.Any,
    ) -> None:
        job.touched = now()

        for k, v in kwargs.items():
            setattr(job, k, v)

        async with self.nullcontext(connection) if connection else self.pool.acquire() as conn:
            if expire_at != -1:
                await conn.execute(
                    f"""
                    UPDATE {self.jobs_table} SET
                      job = $1,
                      status = $2,
                      scheduled = $3,
                      expire_at = $4
                    WHERE key = $5
                    """,
                    self.serialize(job),
                    job.status,
                    job.scheduled,
                    expire_at,
                    job.key,
                )
            else:
                await conn.execute(
                    f"""
                    UPDATE {self.jobs_table} SET
                      job = $1,
                      status = $2,
                      scheduled = $3
                    WHERE key = $4
                    """,
                    self.serialize(job),
                    job.status,
                    job.scheduled,
                    job.key,
                )
            await self.notify(job, conn)

    async def job(self, job_key: str) -> Job | None:
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow(
                f"""
                SELECT job
                FROM {self.jobs_table}
                WHERE key = $1
                """,
                job_key,
            )
            if result:
                return self.deserialize(result['job'])
        return None

    async def jobs(self, job_keys: Iterable[str]) -> t.List[Job | None]:
        keys = list(job_keys)

        async with self.pool.acquire() as conn:
            results = await conn.fetch(
                f"""
                SELECT key, job
                FROM {self.jobs_table}
                WHERE key = ANY($1)
                """,
                keys,
            )
        job_dict = {row['key']: row['job'] for row in results}
        return [self.deserialize(job_dict.get(key)) for key in keys]

    async def iter_jobs(
        self,
        statuses: t.List[Status] = list(Status),
        batch_size: int = 100,
    ) -> t.AsyncIterator[Job]:
        async with self.pool.acquire() as conn:
            last_key = ""

            while True:
                results = await conn.fetch(
                    f"""
                    SELECT key, job
                    FROM {self.jobs_table}
                    WHERE
                      status = ANY($1)
                      AND queue = $2
                      AND key > $3
                    ORDER BY key
                    LIMIT $4
                    """,
                    [status.value for status in statuses],
                    self.name,
                    last_key,
                    batch_size,
                )

                if not results:
                    break

                for row in results:
                    last_key = row['key']
                    job = self.deserialize(row['job'])
                    if job:
                        yield job

    async def abort(self, job: Job, error: str, ttl: float = 5) -> None:
        async with self.pool.acquire() as conn:
            status = await self.get_job_status(job.key, for_update=True, connection=conn)
            if status == Status.QUEUED:
                await self.finish(job, Status.ABORTED, error=error, connection=conn)
            else:
                await self.update(job, status=Status.ABORTING, error=error, connection=conn)

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
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow(
                f"""
                INSERT INTO {self.jobs_table} (key, job, queue, status, scheduled)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (key) DO UPDATE
                SET job = $2, queue = $3, status = $4, scheduled = $5, expire_at = null
                WHERE {self.jobs_table}.status IN ('aborted', 'complete', 'failed')
                  AND $5 > {self.jobs_table}.scheduled
                RETURNING job
                """,
                job.key,
                self.serialize(job),
                self.name,
                job.status,
                job.scheduled or seconds(now()),
            )

            if not result:
                return None

            await self._notify(ENQUEUE_CHANNEL, job.key, conn)

        logger.info("Enqueuing %s", job.info(logger.isEnabledFor(logging.DEBUG)))
        return job

    async def write_stats(self, stats: QueueStats, ttl: int) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {self.stats_table} (worker_id, stats, expire_at)
                VALUES ($1, $2, $3)
                ON CONFLICT (worker_id) DO UPDATE
                SET stats = $2, expire_at = $3
                """,
                self.uuid,
                json.dumps(stats),
                seconds(now()) + ttl,
            )

    async def dequeue_timer(self, poll_interval: int) -> None:
        """Wakes up a single dequeue task every `poll_interval` seconds."""
        while True:
            async with self.cond:
                self.cond.notify(1)
            await asyncio.sleep(poll_interval)

    async def listen_for_enqueues(self, timeout: float | None = None) -> None:
        """Wakes up a single dequeue task when a Postgres enqueue notification is received."""
        async def _listen(conn: PoolConnectionProxy | Connection, pid: int, channel: str, payload: str) -> None:
            async with self.cond:
                self.cond.notify(1)
        async with self.pool.acquire() as conn:
            await conn.add_listener(ENQUEUE_CHANNEL, _listen) 
            if timeout:
                try:
                    await asyncio.sleep(timeout)
                finally:
                    await conn.remove_listener(ENQUEUE_CHANNEL, _listen)
            else:
                # If no timeout, we'll keep listening indefinitely
                while True:
                    await asyncio.sleep(30)  # Sleep for 30 seconds and then check again

    async def get_job_status(
        self,
        key: str,
        for_update: bool = False,
        connection: PoolConnectionProxy | None = None,
    ) -> Status:
        async with self.nullcontext(connection) if connection else self.pool.acquire() as conn:
            result = await conn.fetchval(
                f"""
                SELECT status
                FROM {self.jobs_table}
                WHERE key = $1
                {"FOR UPDATE" if for_update else ""}
                """,
                key,
            )
            assert result
            return result

    async def _retry(self, job: Job, error: str | None) -> None:
        next_retry_delay = job.next_retry_delay()
        if next_retry_delay:
            scheduled = time.time() + next_retry_delay
        else:
            scheduled = job.scheduled or seconds(now())

        await self.update(job, scheduled=int(scheduled), expire_at=None)

    async def _finish(
        self,
        job: Job,
        status: Status,
        *,
        result: t.Any = None,
        error: str | None = None,
        connection: PoolConnectionProxy | None = None,
    ) -> None:
        key = job.key

        async with self.nullcontext(connection) if connection else self.pool.acquire() as conn:
            if job.ttl >= 0:
                expire_at = seconds(now()) + job.ttl if job.ttl > 0 else None
                await self.update(job, status=status, expire_at=expire_at, connection=conn)
            else:
                await conn.execute(
                    f"""
                    DELETE FROM {self.jobs_table}
                    WHERE key = $1
                    """,
                    key,
                )
                await self.notify(job, conn)
            await self._release_job(key)
            try:
                self.queue.task_done()
            except ValueError:
                # Error because task_done() called too many times, which happens in unit tests
                pass

    async def _dequeue(self) -> list[Job]:
        if not self.waiting:
            return []
        jobs = []
        async with self._get_connection() as conn:
            async with conn.transaction():
                results = await conn.fetch(
                    f"""
                    WITH locked_job AS (
                      SELECT key, lock_key
                      FROM {self.jobs_table}
                      WHERE status = 'queued'
                        AND queue = $1
                        AND $2 >= scheduled
                      ORDER BY scheduled
                      LIMIT $3
                      FOR UPDATE SKIP LOCKED
                    )
                    UPDATE {self.jobs_table} SET status = 'active'
                    FROM locked_job
                    WHERE {self.jobs_table}.key = locked_job.key
                      AND pg_try_advisory_lock($4, locked_job.lock_key)
                    RETURNING job
                    """,
                    self.name,
                    math.ceil(seconds(now())),
                    self.waiting,
                    self.job_lock_keyspace,
                )
                for result in results:
                    job = self.deserialize(result['job'])
                    if job:
                        await self.update(job, status=Status.ACTIVE, connection=conn)
                        jobs.append(job)
        return jobs

    async def _notify(
        self, channel: str, payload: t.Any, connection: PoolConnectionProxy | None = None
    ) -> None:
        async with self.nullcontext(connection) if connection else self.pool.acquire() as conn:
            await conn.execute(f"NOTIFY \"{channel}\", '{payload}'")

    @asynccontextmanager
    async def _get_connection(self) -> t.AsyncGenerator:
        assert self.connection
        async with self.connection_lock:
            try:
                # Pool normally performs this check when getting a connection.
                await self.connection.execute("SELECT 1")
            except asyncpg.exceptions.ConnectionDoesNotExistError:
                # The connection is bad so return it to the pool and get a new one.
                await self.pool.release(self.connection)
                self.connection = await self.pool.acquire()
            yield self.connection

    @asynccontextmanager
    async def nullcontext(self, enter_result: t.Any | None = None) -> t.AsyncGenerator:
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
                f"""
                SELECT pg_advisory_unlock($1, lock_key)
                FROM {self.jobs_table}
                WHERE key = ANY($2)
                """,
                self.job_lock_keyspace,
                self.released,
            )
        self.released.clear()