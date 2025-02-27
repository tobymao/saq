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
from saq.utils import now_seconds

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from saq.types import (
        CountKind,
        ListenCallback,
        DumpType,
        LoadType,
        QueueInfo,
        WorkerInfo,
    )

try:
    from psycopg import AsyncConnection, OperationalError
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
        pool: instance of psycopg_pool.AsyncConnectionPool
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
        poll_interval: how often to poll for jobs. (default 1)
            If 0, the queue will not poll for jobs and will only rely on notifications from the server.
            This mean cron jobs will not be picked up in a timely fashion.
        saq_lock_keyspace: The first of two advisory lock keys used by SAQ. (default 0)
            SAQ uses advisory locks for coordinating tasks between its workers, e.g. sweeping.
        job_lock_keyspace: The first of two advisory lock keys used for jobs. (default 1)
        job_lock_sweep: Whether or not the jobs are swept if there's no lock. (default True)
        priorities: The priority range to dequeue. (default (0, 32767))
    """

    @classmethod
    def from_url(cls: type[PostgresQueue], url: str, **kwargs: t.Any) -> PostgresQueue:
        """Create a queue from a postgres url."""
        return cls(
            AsyncConnectionPool(url, check=AsyncConnectionPool.check_connection, open=False),
            **kwargs,
        )

    def __init__(
        self,
        pool: AsyncConnectionPool,
        name: str = "default",
        versions_table: str = VERSIONS_TABLE,
        jobs_table: str = JOBS_TABLE,
        stats_table: str = STATS_TABLE,
        dump: DumpType | None = None,
        load: LoadType | None = None,
        min_size: int = 4,
        max_size: int = 20,
        poll_interval: int = 1,
        saq_lock_keyspace: int = 0,
        job_lock_keyspace: int = 1,
        job_lock_sweep: bool = True,
        priorities: tuple[int, int] = (0, 32767),
    ) -> None:
        super().__init__(name=name, dump=dump, load=load)

        self.versions_table = Identifier(versions_table)
        self.jobs_table = Identifier(jobs_table)
        self.stats_table = Identifier(stats_table)
        self.pool = pool
        self.min_size = min_size
        self.max_size = max_size
        self.poll_interval = poll_interval
        self.saq_lock_keyspace = saq_lock_keyspace
        self.job_lock_keyspace = job_lock_keyspace
        self.job_lock_sweep = job_lock_sweep
        self._priorities = priorities
        self._waiting = 0  # Internal counter of worker tasks waiting for dequeue
        self._dequeue_conn: AsyncConnection | None = None
        self._connection_lock = asyncio.Lock()
        self._releasing: list[str] = []
        self._has_sweep_lock = False
        self._channel = CHANNEL.format(self.name)
        self._listener = ListenMultiplexer(self.pool, self._channel)
        self._dequeue_lock = asyncio.Lock()
        self._listen_lock = asyncio.Lock()

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
        if self.pool._opened:
            return

        await self.pool.open()
        await self.pool.resize(min_size=self.min_size, max_size=self.max_size)
        await self.init_db()

        await super().connect()

    def serialize(self, job: Job) -> bytes | str:
        """Ensure serialized job is in bytes because the job column is of type BYTEA."""
        serialized = self._dump(job.to_dict())
        if isinstance(serialized, str):
            return serialized.encode("utf-8")
        return serialized

    async def disconnect(self) -> None:
        async with self._connection_lock:
            if self._dequeue_conn:
                await self._dequeue_conn.cancel_safe()
                await self.pool.putconn(self._dequeue_conn)
                self._dequeue_conn = None
        await self.pool.close()
        self._has_sweep_lock = False

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
            results = await cursor.fetchall()
        workers: dict[str, WorkerInfo] = {
            worker_id: {
                "stats": stats,
                "metadata": metadata,
                "queue_key": queue_key,
            }
            for worker_id, stats, queue_key, metadata in results
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
                            SELECT job FROM {jobs_table}
                            WHERE status IN ('new', 'deferred', 'queued', 'active')
                              AND queue = %(queue)s
                            """
                        )
                    ).format(jobs_table=self.jobs_table),
                    {"queue": self.name},
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
            async with self._get_dequeue_conn() as conn, conn.cursor() as cursor, conn.transaction():
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
                        WITH locks AS (
                          SELECT objid
                          FROM pg_locks
                          WHERE locktype = 'advisory'
                            AND classid = %(job_lock_keyspace)s
                            AND objsubid = 2 -- key is int pair, not single bigint
                        )
                        SELECT key, job, objid, status
                        FROM {jobs_table}
                        LEFT OUTER JOIN locks
                            ON lock_key = objid
                        WHERE queue = %(queue)s
                          AND status IN ('active', 'aborting');
                        """
                    )
                ).format(
                    jobs_table=self.jobs_table,
                ),
                {
                    "queue": self.name,
                    "job_lock_keyspace": self.job_lock_keyspace,
                },
            )
            results = await cursor.fetchall()

        for key, job_bytes, objid, status in results:
            job = self.deserialize(job_bytes)
            assert job
            if (objid or not self.job_lock_sweep) and not job.stuck:
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

    async def notify(self, job: Job, connection: AsyncConnection | None = None) -> None:
        await self._notify(job.key, job.status, connection)

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
            await self.notify(job, conn)

    async def job(self, job_key: str) -> Job | None:
        async with self.pool.connection() as conn, conn.cursor() as cursor:
            await cursor.execute(
                SQL(
                    dedent(
                        """
                        SELECT job
                        FROM {jobs_table}
                        WHERE key = %(key)s AND queue = %(queue)s
                        """
                    )
                ).format(jobs_table=self.jobs_table),
                {"key": job_key, "queue": self.name},
            )
            job = await cursor.fetchone()
            if job:
                return self.deserialize(job[0])
        return None

    async def jobs(self, job_keys: Iterable[str]) -> t.List[Job | None]:
        keys = list(job_keys)

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
                {"keys": keys},
            )
            results: dict[str, bytes | None] = dict(await cursor.fetchall())
        return [self.deserialize(results.get(key)) for key in keys]

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
                            SELECT key, job
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
                    for key, job_bytes in rows:
                        last_key = key
                        job = self.deserialize(job_bytes)
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
            async with self._get_dequeue_conn() as conn, conn.cursor() as cursor, conn.transaction():
                if not self._waiting:
                    return
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
                        "now": now_seconds(),
                        "limit": self._waiting,
                        "plow": self._priorities[0],
                        "phigh": self._priorities[1],
                    },
                )
                results = await cursor.fetchall()

            for result in results:
                self._job_queue.put_nowait(self.deserialize(result[0]))

            if results:
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
                        SELECT status
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
                return result[0]
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

        async with self.nullcontext(
            connection
        ) if connection else self.pool.connection() as conn, conn.cursor() as cursor:
            if job.ttl >= 0:
                expire_at = now_seconds() + job.ttl if job.ttl > 0 else None
                await self.update(job, status=status, expire_at=expire_at, connection=conn)
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

    async def _notify(
        self, key: str, data: t.Any | None = None, connection: AsyncConnection | None = None
    ) -> None:
        payload = {"key": key}

        if data is not None:
            payload["data"] = data

        async with self.nullcontext(connection) if connection else self.pool.connection() as conn:
            await conn.execute(
                SQL("NOTIFY {channel}, {payload}").format(
                    channel=Identifier(self._channel), payload=json.dumps(payload)
                )
            )

    @asynccontextmanager
    async def _get_dequeue_conn(self) -> t.AsyncGenerator:
        async with self._connection_lock:
            if self._dequeue_conn:
                try:
                    # Pool normally performs this check when getting a connection.
                    await self.pool.check_connection(self._dequeue_conn)
                except OperationalError:
                    # The connection is bad so return it to the pool and get a new one.
                    await self.pool.putconn(self._dequeue_conn)
                    self._dequeue_conn = await self.pool.getconn()
            else:
                self._dequeue_conn = await self.pool.getconn()
            yield self._dequeue_conn

    @asynccontextmanager
    async def nullcontext(self, enter_result: t.Any | None = None) -> t.AsyncGenerator:
        """Async version of contextlib.nullcontext

        Async support has been added to contextlib.nullcontext in Python 3.10.
        """
        yield enter_result

    async def _release_job(self, key: str) -> None:
        self._releasing.append(key)
        if self._connection_lock.locked():
            return
        async with self._get_dequeue_conn() as conn:
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
                {"keys": self._releasing},
            )
            await conn.commit()
        self._releasing.clear()

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
            await conn.commit()

            async for notify in conn.notifies():
                payload = json.loads(notify.payload)
                self.publish(payload["key"], payload)
