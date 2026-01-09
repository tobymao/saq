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
from functools import cached_property
from textwrap import dedent

from saq.errors import MissingDependencyError
from saq.job import (
    Job,
    Status,
)
from saq.multiplexer import Multiplexer
from saq.queue.base import Queue, logger
from saq.queue.postgres_migrations import get_migrations
from saq.utils import now, now_seconds

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from saq.types import (
        CountKind,
        DumpType,
        LoadType,
        QueueInfo,
        WorkerInfo,
    )

try:
    from psycopg import AsyncConnection
    from psycopg.sql import Identifier, SQL
    from psycopg_pool import AsyncConnectionPool
except ModuleNotFoundError as e:
    raise MissingDependencyError(
        "Missing dependencies for Postgres. Install them with `pip install saq[postgres]`."
    ) from e

CHANNEL = "saq:{}"
ENQUEUE = "saq:enqueue"
DEQUEUE = "saq:dequeue"
JOBS_TABLE = "saq_jobs"
STATS_TABLE = "saq_stats"
VERSIONS_TABLE = "saq_versions"


class PostgresQueue(Queue):
    """
    Queue is used to interact with Postgres.

    Args:
        pool: instance of psycopg_pool.AsyncConnectionPool (optional if not passed in, url must be provided)
        url: postgres url (optional if pool is provided)
        name: name of the queue (default "default")
        versions_table: name of the Postgres table SAQ will use to maintain migrations
        jobs_table: name of the Postgres table SAQ will write jobs to (default "saq_jobs")
        stats_table: name of the Postgres table SAQ will write stats to (default "saq_stats")
        dump: lambda that takes a dictionary and outputs bytes (default `json.dumps`)
        load: lambda that takes str or bytes and outputs a python dictionary (default `json.loads`)
        min_size: minimum pool size. (default 4)
            The minimum number of Postgres connections.
        max_size: maximum pool size. (default 20)
        If greater than 0, this limits the maximum number of connections to Postgres.
            Otherwise, maintain `min_size` number of connections.
        saq_lock_keyspace: The first of two advisory lock keys used by SAQ. (default 0)
            SAQ uses advisory locks for coordinating tasks between its workers, e.g. sweeping.
        priorities: The priority range to dequeue. (default (0, 32767))
        swept_error_message: The error message to use when sweeping jobs. (default "swept")
        manage_pool_lifecycle: Whether to have SAQ manage the lifecycle of the connection pool. (default None)
            If None, the pool will be managed if a pool is not provided, otherwise it will not be managed.
    """

    @classmethod
    def from_url(cls: type[PostgresQueue], url: str, **kwargs: t.Any) -> PostgresQueue:
        """Create a queue from a postgres url.

        Args:
            url: The postgres url.
            kwargs: Additional keyword arguments to pass to the constructor.
        """
        return cls(url=url, **kwargs)

    def __init__(
        self,
        pool: AsyncConnectionPool | None = None,
        url: str | None = None,
        name: str = "default",
        versions_table: str = VERSIONS_TABLE,
        jobs_table: str = JOBS_TABLE,
        stats_table: str = STATS_TABLE,
        dump: DumpType | None = None,
        load: LoadType | None = None,
        min_size: int = 4,
        max_size: int = 20,
        saq_lock_keyspace: int = 0,
        priorities: tuple[int, int] = (0, 32767),
        swept_error_message: str | None = None,
        manage_pool_lifecycle: bool | None = None,
    ) -> None:
        super().__init__(name=name, dump=dump, load=load, swept_error_message=swept_error_message)

        if pool is None and url is None:
            raise ValueError("Either pool or url must be provided")
        if pool is not None and url is not None:
            raise ValueError("Either pool or url must be provided, not both")

        self.versions_table = Identifier(versions_table)
        self.jobs_table = Identifier(jobs_table)
        self.stats_table = Identifier(stats_table)
        self.pool = pool or AsyncConnectionPool(
            url,  # type: ignore
            check=AsyncConnectionPool.check_connection,
            open=False,
        )

        if callable(self.pool.kwargs):
            func = t.cast(t.Callable[[], t.Awaitable[t.Dict[str, t.Any]]], self.pool.kwargs)
            kwargs: t.Dict[str, t.Any] = asyncio.run(func())  # type: ignore
            autocommit = kwargs.get("autocommit")
            self.pool.kwargs = lambda: kwargs | {"autocommit": True}  # type: ignore
        else:
            if self.pool.kwargs is None:
                self.pool.kwargs = {}  # type: ignore[unreachable]
            autocommit = self.pool.kwargs.get("autocommit")
            self.pool.kwargs["autocommit"] = True

        if autocommit is False:
            raise ValueError("SAQ Connection pool must have autocommit enabled.")

        self._manage_pool_lifecycle = (
            manage_pool_lifecycle if manage_pool_lifecycle is not None else pool is None
        )
        self.min_size = min_size
        self.max_size = max_size
        self.saq_lock_keyspace = saq_lock_keyspace
        self._priorities = priorities
        self._waiting = 0  # Internal counter of worker tasks waiting for dequeue
        self._has_sweep_lock = False
        self._channel = CHANNEL.format(self.name)
        self._listener = ListenMultiplexer(self.pool, self._channel)
        self._dequeue_lock = asyncio.Lock()
        self._listen_lock = asyncio.Lock()
        self._connected = False

    async def init_db(self) -> None:
        async with self.pool.connection() as conn, conn.cursor() as cursor, conn.transaction():
            await cursor.execute(
                SQL("SELECT pg_try_advisory_lock(%(key1)s, 0)"),
                {"key1": self.saq_lock_keyspace},
            )
            result = await cursor.fetchone()
            if result and not result[0]:
                return

            await cursor.execute(
                SQL(
                    dedent("""
            CREATE TABLE IF NOT EXISTS {versions_table} (
                version INT
            );
                        """)
                ).format(versions_table=self.versions_table)
            )

            migrations = get_migrations(
                jobs_table=self.jobs_table,
                stats_table=self.stats_table,
            )
            target_version = migrations[-1][0]
            await cursor.execute(
                SQL(
                    dedent(
                        """
                    SELECT version FROM {versions_table}
                    """
                    )
                ).format(versions_table=self.versions_table),
            )
            result = await cursor.fetchone()
            if result is not None:
                current_version = result[0]
                if current_version == target_version:
                    return
                if current_version > target_version:
                    raise ValueError("The library version is behind the schema version.")

            current = result[0] if result else 0
            # Find the index of the next migration
            index = next(
                (i for i, migration in enumerate(migrations) if migration[0] > current),
                None,  # default if not found
            )
            if index is None:
                raise ValueError("Could not find the next migration.")
            for migration in migrations[current:]:
                for migration_statement in migration[1]:
                    await cursor.execute(
                        migration_statement,
                    )

            await cursor.execute(
                SQL(
                    dedent(
                        """
                            DELETE FROM {versions_table};
                            INSERT INTO {versions_table} (version) VALUES ({target_version});
                            """
                    )
                ).format(versions_table=self.versions_table, target_version=target_version),
            )

    async def connect(self) -> None:
        if self._connected:
            return
        if self._manage_pool_lifecycle:
            await self.pool.open()
            await self.pool.resize(min_size=self.min_size, max_size=self.max_size)
        await self.init_db()
        await super().connect()
        self._connected = True

    def serialize(self, job: Job) -> bytes | str:
        """Ensure serialized job is in bytes because the job column is of type BYTEA."""
        serialized = self._dump(job.to_dict())
        if isinstance(serialized, str):
            return serialized.encode("utf-8")
        return serialized

    async def disconnect(self) -> None:
        if not self._connected:
            return

        if self._manage_pool_lifecycle:
            await self.pool.close()
        self._has_sweep_lock = False
        self._connected = False

    async def info(self, jobs: bool = False, offset: int = 0, limit: int = 10) -> QueueInfo:
        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                SQL(
                    dedent(
                        """
                        SELECT worker_id, stats, queue_key, metadata
                        FROM {stats_table}
                        WHERE expire_at >= EXTRACT(EPOCH FROM NOW())
                          AND queue_key = %(queue)s
                        """
                    )
                ).format(stats_table=self.stats_table),
                {"queue": self.name},
            )
            rows = await cursor.fetchall()
        workers: dict[str, WorkerInfo] = {
            worker_id: {
                "stats": stats,
                "metadata": metadata,
                "queue_key": queue_key,
            }
            for worker_id, stats, queue_key, metadata in rows
        }

        queued = await self.count("queued")
        active = await self.count("active")
        incomplete = await self.count("incomplete")

        if jobs:
            async with self.pool.connection() as conn, conn.cursor() as cursor:
                await cursor.execute(
                    SQL(
                        dedent(
                            """
                            SELECT job, status FROM {jobs_table}
                            WHERE status IN ('new', 'deferred', 'queued', 'active')
                              AND queue = %(queue)s
                            """
                        )
                    ).format(jobs_table=self.jobs_table),
                    {"queue": self.name},
                )
                rows = await cursor.fetchall()
            deserialized_jobs = (self.deserialize(*row) for row in rows)
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
                            SELECT count(*)
                            FROM {jobs_table}
                            WHERE status = 'queued'
                              AND queue = %(queue)s
                              AND %(now)s >= scheduled
                            """
                        )
                    ).format(jobs_table=self.jobs_table),
                    {"queue": self.name, "now": now_seconds()},
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

    async def schedule(self, lock: int = 1) -> t.List[str]:
        await self._dequeue()
        return []

    async def sweep(self, lock: int = 60, abort: float = 5.0) -> list[str]:
        """Delete jobs and stats past their expiration and sweep stuck jobs"""
        swept = []

        if not self._has_sweep_lock:
            # Attempt to get the sweep lock and hold on to it
            async with self.pool.connection() as conn, conn.cursor() as cursor:
                await cursor.execute(
                    SQL("SELECT pg_try_advisory_lock(%(key1)s, hashtext(%(queue)s))"),
                    {
                        "key1": self.saq_lock_keyspace,
                        "queue": self.name,
                    },
                )
                result = await cursor.fetchone()
            if result and not result[0]:
                # Could not acquire the sweep lock so another worker must already have it
                return []
            self._has_sweep_lock = True

        async with self.pool.connection() as conn, conn.cursor() as cursor:
            now_ts = now_seconds()

            await cursor.execute(
                SQL(
                    dedent(
                        """
                        -- Delete expired jobs
                        DELETE FROM {jobs_table}
                        WHERE queue = %(queue)s
                          AND status IN ('aborted', 'complete', 'failed')
                          AND %(now)s >= expire_at;
                        """
                    )
                ).format(
                    jobs_table=self.jobs_table,
                    stats_table=self.stats_table,
                ),
                {"queue": self.name, "now": now_ts},
            )

            await cursor.execute(
                SQL(
                    dedent(
                        """
                        -- Delete expired stats
                        DELETE FROM {stats_table}
                        WHERE %(now)s >= expire_at;
                        """
                    )
                ).format(
                    jobs_table=self.jobs_table,
                    stats_table=self.stats_table,
                ),
                {"now": now_ts},
            )

            await cursor.execute(
                SQL(
                    dedent(
                        """
                        SELECT key, job, status
                        FROM {jobs_table}
                        WHERE queue = %(queue)s
                          AND status IN ('active', 'aborting');
                        """
                    )
                ).format(
                    jobs_table=self.jobs_table,
                ),
                {
                    "queue": self.name,
                },
            )
            rows = await cursor.fetchall()

        for key, job_bytes, status in rows:
            job = self.deserialize(job_bytes, status)
            assert job
            if not job.stuck:
                continue

            swept.append(key)
            logger.info("Sweeping %s", job.info(logger.isEnabledFor(logging.DEBUG)))

            await self.abort(job, error=self.swept_error_message)

            try:
                await job.refresh(abort)
            except asyncio.TimeoutError:
                logger.info("Could not abort job %s", key)

            if job.retryable:
                await self.retry(job, error=self.swept_error_message)
            else:
                await self.finish(job, Status.ABORTED, error=self.swept_error_message)
        return swept

    async def _update(self, job: Job, status: Status | None = None, **kwargs: t.Any) -> None:
        expire_at = kwargs.pop("expire_at", -1)
        connection = kwargs.pop("connection", None)

        async with self.nullcontext(
            connection
        ) if connection else self.pool.connection() as conn, conn.transaction():
            if not status:
                status = await self.get_job_status(job.key, for_update=True, connection=conn)

            job.status = status or job.status

            await conn.execute(
                SQL(
                    dedent(
                        """
                        UPDATE {jobs_table} SET
                          job = %(job)s
                          ,status = %(status)s
                          ,scheduled = %(scheduled)s
                          {expire_at}
                        WHERE key = %(key)s
                        """
                    )
                ).format(
                    jobs_table=self.jobs_table,
                    expire_at=SQL(",expire_at = %(expire_at)s" if expire_at != -1 else ""),
                ),
                {
                    "job": self.serialize(job),
                    "status": job.status,
                    "key": job.key,
                    "scheduled": job.scheduled,
                    "expire_at": expire_at,
                },
            )

    async def job(self, job_key: str) -> Job | None:
        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                SQL(
                    dedent(
                        """
                        SELECT job, status
                        FROM {jobs_table}
                        WHERE key = %(key)s AND queue = %(queue)s
                        """
                    )
                ).format(jobs_table=self.jobs_table),
                {"key": job_key, "queue": self.name},
            )
            row = await cursor.fetchone()
            if row:
                return self.deserialize(*row)
        return None

    async def jobs(self, job_keys: Iterable[str]) -> t.List[Job | None]:
        keys = list(job_keys)

        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                SQL(
                    dedent(
                        """
                        SELECT key, job, status
                        FROM {jobs_table}
                        WHERE key = ANY(%(keys)s)
                        """
                    )
                ).format(jobs_table=self.jobs_table),
                {"keys": keys},
            )
            results = {r[0]: r[1:] for r in await cursor.fetchall()}
        return [self.deserialize(*(results.get(key) or [None])) for key in keys]

    async def iter_jobs(
        self,
        statuses: t.List[Status] = list(Status),
        batch_size: int = 100,
    ) -> t.AsyncIterator[Job]:
        async with self.pool.connection() as conn, conn.cursor() as cursor:
            last_key = ""

            while True:
                await cursor.execute(
                    SQL(
                        dedent(
                            """
                            SELECT key, job, status
                            FROM {jobs_table}
                            WHERE
                              status = ANY(%(statuses)s)
                              AND queue = %(queue)s
                              AND key > %(last_key)s
                            ORDER BY key
                            LIMIT %(batch_size)s
                            """
                        )
                    ).format(jobs_table=self.jobs_table),
                    {
                        "statuses": statuses,
                        "queue": self.name,
                        "batch_size": batch_size,
                        "last_key": last_key,
                    },
                )

                rows = await cursor.fetchall()

                if rows:
                    for key, job_bytes, status in rows:
                        last_key = key
                        job = self.deserialize(job_bytes, status)
                        if job:
                            yield job
                else:
                    break

    async def abort(self, job: Job, error: str, ttl: float = 5) -> None:
        async with self.pool.connection() as conn:
            status = await self.get_job_status(job.key, for_update=True, connection=conn)
            if not status or status == Status.QUEUED:
                await self.finish(job, Status.ABORTED, error=error, connection=conn)
            else:
                await self.update(job, status=Status.ABORTING, error=error, connection=conn)

    async def dequeue(self, timeout: float = 0.0, poll_interval: float = 0.0) -> Job | None:
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
                    if poll_interval > 0.0:

                        async def _poll() -> Job | None:
                            while self._connected:
                                await self._dequeue()

                                if not self._job_queue.empty():
                                    return self._job_queue.get_nowait()

                                await asyncio.sleep(poll_interval)

                            return None

                        job = await (
                            asyncio.wait_for(_poll(), timeout) if timeout > 0.0 else _poll()
                        )
                    else:
                        async for payload in self._listener.listen(
                            ENQUEUE, DEQUEUE, timeout=timeout
                        ):
                            if payload == ENQUEUE:
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
            async with self.pool.connection() as conn, conn.transaction(), conn.cursor() as cursor:
                if not self._waiting:
                    return
                await cursor.execute(
                    SQL(
                        dedent(
                            """
                            WITH locked_job AS (
                              SELECT key
                              FROM {jobs_table}
                              WHERE status = 'queued'
                                AND queue = %(queue)s
                                AND %(now)s >= scheduled
                                AND priority BETWEEN %(plow)s AND %(phigh)s
                                AND group_key NOT IN (
                                  SELECT DISTINCT group_key
                                  FROM {jobs_table}
                                  WHERE status = 'active'
                                    AND queue = %(queue)s
                                    AND group_key IS NOT NULL
                                )
                              ORDER BY priority, scheduled
                              LIMIT %(limit)s
                              FOR UPDATE SKIP LOCKED
                            )
                            UPDATE {jobs_table} SET status = 'active'
                            FROM locked_job
                            WHERE {jobs_table}.key = locked_job.key
                            RETURNING job
                            """
                        )
                    ).format(
                        jobs_table=self.jobs_table,
                    ),
                    {
                        "queue": self.name,
                        "now": now_seconds(),
                        "limit": self._waiting,
                        "plow": self._priorities[0],
                        "phigh": self._priorities[1],
                    },
                )

                rows = await cursor.fetchall()
                dequeued = now()

                for row in rows:
                    # there can be a race condition where a job is swept right after it is dequeued
                    # but before it's updated for processing
                    job = self.deserialize(row[0], Status.ACTIVE)
                    assert job
                    job.started = dequeued
                    job.touched = dequeued
                    await self._update(job, status=Status.ACTIVE, connection=conn)
                    self._job_queue.put_nowait(job)

            if rows:
                await self._notify(DEQUEUE)

    async def _enqueue(self, job: Job) -> Job | None:
        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                SQL(
                    dedent(
                        """
                        INSERT INTO {jobs_table} (
                          key,
                          job,
                          queue,
                          status,
                          priority,
                          group_key,
                          scheduled
                        )
                        VALUES (
                          %(key)s,
                          %(job)s,
                          %(queue)s,
                          %(status)s,
                          %(priority)s,
                          %(group_key)s,
                          %(scheduled)s
                        )
                        ON CONFLICT (key) DO UPDATE
                        SET
                          job = %(job)s,
                          queue = %(queue)s,
                          status = %(status)s,
                          priority = %(priority)s,
                          group_key = %(group_key)s,
                          scheduled = %(scheduled)s,
                          expire_at = null
                        WHERE
                          {jobs_table}.status IN ('aborted', 'complete', 'failed')
                          AND %(scheduled)s > {jobs_table}.scheduled
                        RETURNING 1
                        """
                    )
                ).format(jobs_table=self.jobs_table),
                {
                    "key": job.key,
                    "job": self.serialize(job),
                    "queue": self.name,
                    "status": job.status,
                    "priority": job.priority,
                    "group_key": job.group_key,
                    "scheduled": job.scheduled or int(now_seconds()),
                },
            )

            if not await cursor.fetchone():
                return None
            await self._notify(ENQUEUE, connection=conn)
        logger.info("Enqueuing %s", job.info(logger.isEnabledFor(logging.DEBUG)))
        return job

    async def write_worker_info(
        self,
        worker_id: str,
        info: WorkerInfo,
        ttl: int,
    ) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                SQL(
                    dedent(
                        """
                        INSERT INTO {stats_table} (worker_id, stats, queue_key, metadata, expire_at)
                        VALUES (%(worker_id)s, %(stats)s, %(queue_key)s, %(metadata)s, EXTRACT(EPOCH FROM NOW()) + %(ttl)s)
                        ON CONFLICT (worker_id) DO UPDATE
                        SET stats = %(stats)s, queue_key = %(queue_key)s, metadata = %(metadata)s, expire_at = EXTRACT(EPOCH FROM NOW()) + %(ttl)s
                        """
                    )
                ).format(stats_table=self.stats_table),
                {
                    "worker_id": worker_id,
                    "stats": json.dumps(info["stats"]),
                    "ttl": ttl,
                    "queue_key": info["queue_key"],
                    "metadata": json.dumps(info["metadata"]),
                },
            )

    async def get_job_status(
        self,
        key: str,
        for_update: bool = False,
        connection: AsyncConnection | None = None,
    ) -> Status | None:
        async with self.nullcontext(
            connection
        ) if connection else self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                SQL(
                    dedent(
                        """
                        SELECT UPPER(status)
                        FROM {jobs_table}
                        WHERE key = %(key)s
                        {for_update}
                        """
                    )
                ).format(
                    jobs_table=self.jobs_table,
                    for_update=SQL("FOR UPDATE" if for_update else ""),
                ),
                {
                    "key": key,
                },
            )
            result = await cursor.fetchone()
            if result:
                return Status[result[0]]
            return None

    async def _retry(self, job: Job, error: str | None) -> None:
        next_retry_delay = job.next_retry_delay()
        if next_retry_delay:
            scheduled = time.time() + next_retry_delay
        else:
            scheduled = job.scheduled or now_seconds()

        await self.update(job, status=Status.QUEUED, scheduled=int(scheduled), expire_at=None)

    async def _finish(
        self,
        job: Job,
        status: Status,
        *,
        result: t.Any = None,
        error: str | None = None,
        connection: AsyncConnection | None = None,
    ) -> None:
        key = job.key

        async with self.nullcontext(connection) if connection else self.pool.connection() as conn:
            if job.ttl >= 0:
                expire_at = now_seconds() + job.ttl if job.ttl > 0 else None
                await self.update(job, status=status, expire_at=expire_at, connection=conn)
            else:
                await conn.execute(
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

    async def _notify(self, key: str, connection: AsyncConnection | None = None) -> None:
        async with self.nullcontext(connection) if connection else self.pool.connection() as conn:
            await conn.execute(
                SQL("NOTIFY {channel}, {key}").format(channel=Identifier(self._channel), key=key)
            )

    @asynccontextmanager
    async def nullcontext(self, enter_result: t.Any | None = None) -> t.AsyncGenerator:
        """Async version of contextlib.nullcontext

        Async support has been added to contextlib.nullcontext in Python 3.10.
        """
        yield enter_result

    @cached_property
    def _job_queue(self) -> asyncio.Queue:
        return asyncio.Queue()


class ListenMultiplexer(Multiplexer):
    def __init__(self, pool: AsyncConnectionPool, key: str) -> None:
        super().__init__()
        self.pool = pool
        self.key = key

    async def _start(self) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(SQL("LISTEN {}").format(Identifier(self.key)))

            async for notify in conn.notifies():
                self.publish(notify.payload, notify.payload)
