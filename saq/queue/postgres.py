"""
Postgres Queue
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import typing as t
from contextlib import asynccontextmanager
from textwrap import dedent

from saq.errors import MissingDependencyError
from saq.job import (
    Job,
    Status,
)
from saq.multiplexer import Multiplexer
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
    from asyncpg import Pool, create_pool, Connection
    from asyncpg.exceptions import ConnectionDoesNotExistError, InterfaceError
    from asyncpg.pool import PoolConnectionProxy
except ModuleNotFoundError as e:
    raise MissingDependencyError(
        "Missing dependencies for Postgres. Install them with `pip install saq[postgres]`."
    ) from e

CHANNEL = "saq:{}"
ENQUEUE = "saq:enqueue"
DEQUEUE = "saq:dequeue"
JOBS_TABLE = "saq_jobs"
STATS_TABLE = "saq_stats"

ContextT = t.TypeVar("ContextT")


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

        poll_interval: how often to poll for jobs. (default 1)
            If 0, the queue will not poll for jobs and will only rely on notifications from the server.
            This mean cron jobs will not be picked up in a timely fashion.
        saq_lock_keyspace: The first of two advisory lock keys used by SAQ. (default 0)
            SAQ uses advisory locks for coordinating tasks between its workers, e.g. sweeping.
        job_lock_keyspace: The first of two advisory lock keys used for jobs. (default 1)
        priorities: The priority range to dequeue (default (0, 32767))
    """

    @classmethod
    def from_url(  # pyright: ignore[reportIncompatibleMethodOverride]
        cls: type[PostgresQueue],
        url: str,
        min_size: int = 4,
        max_size: int = 20,
        **kwargs: t.Any,
    ) -> PostgresQueue:
        """Create a queue from a postgres url.

        Args:
            url: connection string for the databases
            min_size: minimum pool size. (default 4)
                The minimum number of Postgres connections.
            max_size: maximum pool size. (default 20)
                If greater than 0, this limits the maximum number of connections to Postgres.
                Otherwise, maintain `min_size` number of connections.

        """
        return cls(create_pool(dsn=url, min_size=min_size, max_size=max_size), **kwargs)

    def __init__(
        self,
        pool: Pool[t.Any],
        name: str = "default",
        jobs_table: str = JOBS_TABLE,
        stats_table: str = STATS_TABLE,
        dump: DumpType | None = None,
        load: LoadType | None = None,
        poll_interval: int = 1,
        saq_lock_keyspace: int = 0,
        job_lock_keyspace: int = 1,
        priorities: tuple[int, int] = (0, 32767),
    ) -> None:
        super().__init__(name=name, dump=dump, load=load)

        self.jobs_table = jobs_table
        self.stats_table = stats_table
        self.pool = pool
        self.poll_interval = poll_interval
        self.saq_lock_keyspace = saq_lock_keyspace
        self.job_lock_keyspace = job_lock_keyspace
        self._priorities = priorities

        self._job_queue: asyncio.Queue = asyncio.Queue()
        self._waiting = 0  # Internal counter of worker tasks waiting for dequeue
        self._dequeue_conn: PoolConnectionProxy | None = None
        self._connection_lock = asyncio.Lock()
        self._releasing: list[str] = []
        self._has_sweep_lock = False
        self._channel = CHANNEL.format(self.name)
        self._listener = ListenMultiplexer(self.pool, self._channel)
        self._dequeue_lock = asyncio.Lock()
        self._listen_lock = asyncio.Lock()

    @asynccontextmanager
    async def with_connection(
        self, connection: PoolConnectionProxy | None = None
    ) -> t.AsyncGenerator[PoolConnectionProxy]:
        async with self.nullcontext(connection) if connection else self.pool.acquire() as conn:  # type: ignore[attr-defined]
            yield conn

    async def init_db(self) -> None:
        async with self.with_connection() as conn, conn.transaction():
            cursor = await conn.cursor(
                "SELECT pg_try_advisory_lock($1, 0)",
                self.saq_lock_keyspace,
            )
            result = await cursor.fetchrow()

            if result and not result[0]:
                return
            for statement in DDL_STATEMENTS:
                await conn.execute(
                    statement.format(jobs_table=self.jobs_table, stats_table=self.stats_table)
                )

    async def connect(self) -> None:
        if self._dequeue_conn:
            return
        # the return from the `from_url` call must be awaited.  The loop isn't running at the time `from_url` is called, so this seemed to make the most sense
        self.pool._loop = asyncio.get_event_loop()  # type: ignore[attr-defined]
        await self.pool
        self._dequeue_conn = await self.pool.acquire()
        await self.init_db()

    def serialize(self, job: Job) -> bytes | str:
        """Ensure serialized job is in bytes because the job column is of type BYTEA."""
        serialized = self._dump(job.to_dict())
        if isinstance(serialized, str):
            return serialized.encode("utf-8")
        return serialized

    async def disconnect(self) -> None:
        async with self._connection_lock:
            if self._dequeue_conn:
                await self.pool.release(self._dequeue_conn)
                self._dequeue_conn = None
        await self.pool.close()
        self._has_sweep_lock = False

    async def info(self, jobs: bool = False, offset: int = 0, limit: int = 10) -> QueueInfo:
        workers: dict[str, dict[str, t.Any]] = {}
        async with self.with_connection() as conn:
            results = await conn.fetch(
                dedent(f"""
                SELECT worker_id, stats FROM {self.stats_table}
                WHERE NOW() <= TO_TIMESTAMP(expire_at)
                """)
            )
        for record in results:
            workers[record.get("worker_id")] = record.get("stats")
        queued = await self.count("queued")
        active = await self.count("active")
        incomplete = await self.count("incomplete")

        if jobs:
            async with self.with_connection() as conn:
                results = await conn.fetch(
                    dedent(f"""
                    SELECT job FROM {self.jobs_table}
                    WHERE status IN ('new', 'deferred', 'queued', 'active')
                    """)
                )
            deserialized_jobs = (self.deserialize(result["job"]) for result in results)
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
        async with self.with_connection() as conn:
            if kind == "queued":
                result = await conn.fetchval(
                    dedent(f"""
                    SELECT count(*) 
                    FROM {self.jobs_table}
                    WHERE status = 'queued'
                      AND queue = $1
                      AND NOW() >= TO_TIMESTAMP(scheduled)
                    """),
                    self.name,
                )
            elif kind == "active":
                result = await conn.fetchval(
                    dedent(f"""
                    SELECT count(*) 
                    FROM {self.jobs_table}
                    WHERE status = 'active'
                      AND queue = $1
                    """),
                    self.name,
                )
            elif kind == "incomplete":
                result = await conn.fetchval(
                    dedent(f"""
                    SELECT count(*) 
                    FROM {self.jobs_table}
                    WHERE status IN ('new', 'deferred', 'queued', 'active')
                      AND queue = $1
                    """),
                    self.name,
                )
            else:
                raise ValueError(f"Can't count unknown type {kind}")

            return result

    async def schedule(self, lock: int = 1) -> t.List[str]:
        await self._dequeue()
        return []

    async def sweep(self, lock: int = 60, abort: float = 5.0) -> list[str]:
        """Delete jobs and stats past their expiration and sweep stuck jobs"""
        swept = []

        if not self._has_sweep_lock:
            # Attempt to get the sweep lock and hold on to it
            async with self._get_dequeue_conn() as conn:
                result = await conn.fetchval(
                    dedent("SELECT pg_try_advisory_lock($1, hashtext($2))"),
                    self.saq_lock_keyspace,
                    self.name,
                )
            if not result:
                # Could not acquire the sweep lock so another worker must already have it
                return []
            self._has_sweep_lock = True

        async with self.with_connection() as conn, conn.transaction():
            await conn.execute(
                dedent(f"""
                -- Delete expired jobs
                DELETE FROM {self.jobs_table}
                WHERE queue = $1
                AND status IN ('aborted', 'complete', 'failed')
                AND NOW() >= TO_TIMESTAMP(expire_at)
                """),
                self.name,
            )
            await conn.execute(
                dedent(f"""
                -- Delete expired stats
                DELETE FROM {self.stats_table}
                WHERE NOW() >= TO_TIMESTAMP(expire_at);
                """),
            )
            results = await conn.fetch(
                dedent(
                    f"""
                        WITH locks AS (
                          SELECT objid
                          FROM pg_locks
                          WHERE locktype = 'advisory'
                            AND classid = $1
                            AND objsubid = 2 -- key is int pair, not single bigint
                        )
                        SELECT key, job, objid, status
                        FROM {self.jobs_table}
                        LEFT OUTER JOIN locks
                            ON lock_key = objid
                        WHERE queue = $2
                          AND status IN ('active', 'aborting');
                        """
                ),
                self.job_lock_keyspace,
                self.name,
            )

        for row in results:
            key, job_bytes, objid, status = row.values()
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

        async for message in self._listener.listen(*job_keys, timeout=timeout):
            job_key = message["key"]
            status = Status[message["data"].upper()]
            if asyncio.iscoroutinefunction(callback):
                stop = await callback(job_key, status)
            else:
                stop = callback(job_key, status)
            if stop:
                break

    async def notify(self, job: Job, connection: PoolConnectionProxy | None = None) -> None:
        await self._notify(job.key, job.status, connection)

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
        async with self.with_connection(connection) as conn, conn.transaction():
            if expire_at != -1:
                await conn.execute(
                    dedent(f"""
                    UPDATE {self.jobs_table}
                    SET job=$1, status = $2, scheduled = $3, expire_at = $4
                    WHERE key = $5
                    """),
                    self.serialize(job),
                    job.status,
                    job.scheduled,
                    expire_at,
                    job.key,
                )
            else:
                await conn.execute(
                    dedent(f"""
                    UPDATE {self.jobs_table}
                    SET job=$1, status = $2, scheduled=$3
                    WHERE key = $4
                    """),
                    self.serialize(job),
                    job.status,
                    job.scheduled,
                    job.key,
                )
            await self.notify(job, conn)

    async def job(self, job_key: str) -> Job | None:
        async with self.with_connection() as conn, conn.transaction():
            cursor = await conn.cursor(f"SELECT job FROM {self.jobs_table} WHERE key = $1", job_key)
            record = await cursor.fetchrow()
            return self.deserialize(record.get("job")) if record else None

    async def jobs(self, job_keys: Iterable[str]) -> t.List[Job | None]:
        keys = list(job_keys)
        results: dict[str, bytes | None] = {}
        async with self.with_connection() as conn, conn.transaction():
            async for record in conn.cursor(
                f"SELECT key, job FROM {self.jobs_table} WHERE key = ANY($1)", keys
            ):
                results[record.get("key")] = record.get("job")
        return [self.deserialize(results.get(key)) for key in keys]

    async def iter_jobs(
        self,
        statuses: t.List[Status] = list(Status),
        batch_size: int = 100,
    ) -> t.AsyncIterator[Job]:
        async with self.with_connection() as conn, conn.transaction():
            last_key = ""
            while True:
                rows = await conn.fetch(
                    dedent(f"""
                           SELECT key, job
                           FROM {self.jobs_table}
                           WHERE status = ANY($1)
                             AND queue = $2
                             AND key > $3
                           ORDER BY key
                           LIMIT $4"""),
                    statuses,
                    self.name,
                    last_key,
                    batch_size,
                )
                if rows:
                    for row in rows:
                        last_key = row.get("key")
                        job = self.deserialize(row.get("job"))
                        if job:
                            yield job
                else:
                    break

    async def abort(self, job: Job, error: str, ttl: float = 5) -> None:
        async with self.with_connection() as conn:
            status = await self.get_job_status(job.key, for_update=True, connection=conn)
            if status == Status.QUEUED:
                await self.finish(job, Status.ABORTED, error=error, connection=conn)
            else:
                await self.update(job, status=Status.ABORTING, error=error, connection=conn)

    async def dequeue(self, timeout: float = 0) -> Job | None:
        job = None

        try:
            self._waiting += 1

            if self._job_queue.empty():
                await self._dequeue()

            if not self._job_queue.empty():
                job = self._job_queue.get_nowait()
            elif self._listen_lock.locked():
                job = await (
                    asyncio.wait_for(self._job_queue.get(), timeout)
                    if timeout > 0
                    else self._job_queue.get()
                )
            else:
                async with self._listen_lock:
                    async for payload in self._listener.listen(ENQUEUE, DEQUEUE, timeout=timeout):
                        if payload["key"] == ENQUEUE:
                            await self._dequeue()

                        if not self._job_queue.empty():
                            job = self._job_queue.get_nowait()
                            break
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass
        finally:
            self._waiting -= 1

        if job:
            self._job_queue.task_done()

        return job

    async def _dequeue(self) -> None:
        if self._dequeue_lock.locked():
            return

        async with self._dequeue_lock:
            async with self._get_dequeue_conn() as conn, conn.transaction():
                if not self._waiting:
                    return
                should_notify = False
                async for record in conn.cursor(
                    dedent(f"""
                    WITH locked_job AS (
                      SELECT key, lock_key
                      FROM {self.jobs_table}
                      WHERE status = 'queued'
                        AND queue = $1
                        AND NOW() >= TO_TIMESTAMP(scheduled)
                        AND priority BETWEEN $2 AND $3
                        AND group_key NOT IN (
                                  SELECT DISTINCT group_key
                                  FROM  {self.jobs_table}
                                  WHERE status = 'active'
                                    AND queue = $1
                                    AND group_key IS NOT NULL
                                )
                      ORDER BY priority, scheduled
                      LIMIT $4
                      FOR UPDATE SKIP LOCKED
                    )
                    UPDATE {self.jobs_table} SET status = 'active'
                    FROM locked_job
                    WHERE {self.jobs_table}.key = locked_job.key
                      AND pg_try_advisory_lock({self.job_lock_keyspace}, locked_job.lock_key)
                    RETURNING job
                    """),
                    self.name,
                    self._priorities[0],
                    self._priorities[1],
                    self._waiting,
                ):
                    should_notify = True
                    self._job_queue.put_nowait(self.deserialize(record.get("job")))
            if should_notify:
                await self._notify(DEQUEUE)

    async def _enqueue(self, job: Job) -> Job | None:
        async with self.with_connection() as conn, conn.transaction():
            cursor = await conn.cursor(
                f"""
                INSERT INTO {self.jobs_table} (
                    key,
                    job,
                    queue,
                    status,
                    priority,
                    group_key,
                    scheduled
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (key) DO UPDATE
                SET
                  job = $2,
                  queue = $3,
                  status = $4,
                  priority = $5,
                  group_key = $6,
                  scheduled = $7,
                  expire_at = null
                WHERE
                  {self.jobs_table}.status IN ('aborted', 'complete', 'failed')
                  AND $7 > {self.jobs_table}.scheduled
                RETURNING 1
                """,
                job.key,
                self.serialize(job),
                self.name,
                job.status,
                job.priority,
                str(job.group_key),
                job.scheduled or int(seconds(now())),
            )
            if not await cursor.fetchrow():
                return None
            await self._notify(ENQUEUE, connection=conn)
        logger.info("Enqueuing %s", job.info(logger.isEnabledFor(logging.DEBUG)))
        return job

    async def write_stats(self, stats: QueueStats, ttl: int) -> None:
        async with self.with_connection() as conn:
            await conn.execute(
                dedent(f"""
                INSERT INTO {self.stats_table} (worker_id, stats, expire_at)
                VALUES ($1, $2, EXTRACT(EPOCH FROM NOW()) + $3)
                ON CONFLICT (worker_id) DO UPDATE
                SET stats = $2, expire_at = EXTRACT(EPOCH FROM NOW()) + $3
                """),
                self.uuid,
                json.dumps(stats),
                ttl,
            )

    async def get_job_status(
        self,
        key: str,
        for_update: bool = False,
        connection: PoolConnectionProxy | None = None,
    ) -> Status:
        async with self.with_connection(connection) as conn:
            result = await conn.fetchval(
                dedent(f"""
                SELECT status
                FROM {self.jobs_table}
                WHERE key = $1
                {"FOR UPDATE" if for_update else ""}
                """),
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

        async with self.with_connection(connection) as conn:
            if job.ttl >= 0:
                expire_at = int(seconds(now())) + job.ttl if job.ttl > 0 else None
                await self.update(job, status=status, expire_at=expire_at, connection=conn)
            else:
                await conn.execute(
                    dedent(f"""
                    DELETE FROM {self.jobs_table}
                    WHERE key = $1
                    """),
                    key,
                )
                await self.notify(job, conn)
        await self._release_job(key)

    async def _notify(
        self,
        key: str,
        data: t.Any | None = None,
        connection: PoolConnectionProxy | None = None,
    ) -> None:
        payload = {"key": key}

        if data is not None:
            payload["data"] = data

        async with self.with_connection(connection) as conn:
            await conn.execute(f"NOTIFY \"{self._channel}\", '{json.dumps(payload)}'")

    @asynccontextmanager
    async def _get_dequeue_conn(self) -> t.AsyncGenerator:
        async with self._connection_lock:
            if self._dequeue_conn:
                try:
                    # Pool normally performs this check when getting a connection.
                    await self._dequeue_conn.execute("SELECT 1")
                except (ConnectionDoesNotExistError, InterfaceError):
                    # The connection is bad so return it to the pool and get a new one.
                    await self.pool.release(self._dequeue_conn)
                    self._dequeue_conn = await self.pool.acquire()
            else:
                self._dequeue_conn = await self.pool.acquire()

            yield self._dequeue_conn

    @asynccontextmanager
    async def nullcontext(self, enter_result: ContextT) -> t.AsyncGenerator[ContextT]:
        """Async version of contextlib.nullcontext

        Async support has been added to contextlib.nullcontext in Python 3.10.
        """
        yield enter_result

    async def _release_job(self, key: str) -> None:
        self._releasing.append(key)
        if self._connection_lock.locked():
            return
        async with self._get_dequeue_conn() as conn:
            txn = conn.transaction()
            await txn.start()
            await conn.execute(
                f"""
                SELECT pg_advisory_unlock({self.job_lock_keyspace}, lock_key)
                FROM {self.jobs_table}
                WHERE key = ANY($1)
                """,
                self._releasing,
            )
            await txn.commit()
        self._releasing.clear()


class ListenMultiplexer(Multiplexer):
    def __init__(self, pool: Pool, key: str) -> None:
        super().__init__()
        self.pool = pool
        self.key = key
        self._connection: PoolConnectionProxy | None = None

    async def _start(self) -> None:
        if self._connection is None:
            self._connection = await self.pool.acquire()
        txn = self._connection.transaction()
        await txn.start()
        await self._connection.add_listener(self.key, self._notify_callback)
        await txn.commit()

    async def _close(self) -> None:
        if self._connection:
            await self._connection.remove_listener(self.key, self._notify_callback)
            await self._connection.close()
            self._connection = None

    async def _notify_callback(
        self,
        connection: Connection | PoolConnectionProxy,
        pid: int,
        channel: str,
        payload: t.Any,
    ) -> None:
        payload_data = json.loads(payload)
        self.publish(payload_data["key"], payload_data)
