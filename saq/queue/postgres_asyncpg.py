"""
Postgres Queue using asyncpg
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from textwrap import dedent
from typing import TYPE_CHECKING, Any, AsyncIterator, List, TypeVar

from saq.errors import MissingDependencyError
from saq.job import Job, Status
from saq.multiplexer import Multiplexer
from saq.queue.base import Queue, logger
from saq.queue.postgres_ddl import DDL_STATEMENTS
from saq.types import ListenCallback
from saq.utils import now, seconds

if TYPE_CHECKING:
    from collections.abc import Iterable

    from saq.types import CountKind, DumpType, LoadType, QueueInfo, QueueStats
try:
    from asyncpg import Pool, create_pool, Connection
    from asyncpg.exceptions import ConnectionDoesNotExistError, InterfaceError
    from asyncpg.pool import PoolConnectionProxy

except ModuleNotFoundError as e:
    raise MissingDependencyError(
        "Missing dependencies for Postgres. Install them with `pip install saq[asyncpg]`."
    ) from e

CHANNEL = "saq:{}"
ENQUEUE = "saq:enqueue"
DEQUEUE = "saq:dequeue"
JOBS_TABLE = "saq_jobs"
STATS_TABLE = "saq_stats"

ContextT = TypeVar("ContextT")


class PostgresAsyncpgQueue(Queue):
    """
    Queue is used to interact with Postgres using asyncpg.
    """

    @classmethod
    def from_url(  # pyright: ignore[reportIncompatibleMethodOverride]
        cls: type[PostgresAsyncpgQueue],
        url: str,
        min_size: int = 4,
        max_size: int = 20,
        **kwargs: Any,
    ) -> PostgresAsyncpgQueue:
        """Create a queue from a postgres url."""
        pool = create_pool(dsn=url, min_size=min_size, max_size=max_size)
        return cls(pool, **kwargs)

    def __init__(
        self,
        pool: Pool[Any],
        name: str = "default",
        jobs_table: str = JOBS_TABLE,
        stats_table: str = STATS_TABLE,
        dump: DumpType | None = None,
        load: LoadType | None = None,
        poll_interval: int = 1,
        saq_lock_keyspace: int = 0,
        job_lock_keyspace: int = 1,
    ) -> None:
        super().__init__(name=name, dump=dump, load=load)

        self.jobs_table = jobs_table
        self.stats_table = stats_table
        self.pool = pool
        self.poll_interval = poll_interval
        self.saq_lock_keyspace = saq_lock_keyspace
        self.job_lock_keyspace = job_lock_keyspace

        self._job_queue: asyncio.Queue = asyncio.Queue()
        self._waiting = 0
        self._dequeue_conn: PoolConnectionProxy | None = None
        self._connection_lock = asyncio.Lock()
        self._releasing: list[str] = []
        self._has_sweep_lock = False
        self._channel = CHANNEL.format(self.name)
        self._listener = ListenMultiplexer(self.pool, self._channel)
        self._dequeue_lock = asyncio.Lock()
        self._listen_lock = asyncio.Lock()

    async def init_db(self) -> None:
        async with self.with_connection() as conn, conn.transaction():
            cursor = await conn.cursor(
                 "SELECT pg_try_advisory_lock($1, 0)",  self.saq_lock_keyspace,
            )
            result = await cursor.fetchrow()

            if result and not result[0]:
                return
            for statement in DDL_STATEMENTS:
                await conn.execute(
                    statement.format(
                        jobs_table=self.jobs_table, stats_table=self.stats_table
                    )
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
        serialized = self._dump(job.to_dict())
        if isinstance(serialized, str):
            return serialized.encode("utf-8")
        return serialized
    
    @asynccontextmanager
    async def with_connection(
        self, connection: PoolConnectionProxy | None = None
    ) -> AsyncGenerator[PoolConnectionProxy]:
        async with self._nullcontext(
            connection
        ) if connection else self.pool.acquire() as conn:  # type: ignore[attr-defined]
            yield conn

    async def disconnect(self) -> None:
        async with self._connection_lock:
            if self._dequeue_conn:
                await self.pool.release(self._dequeue_conn)
                self._dequeue_conn = None
        await self.pool.close()
        self._has_sweep_lock = False

    async def info(
        self, jobs: bool = False, offset: int = 0, limit: int = 10
    ) -> QueueInfo:
        workers: dict[str, dict[str, Any]] = {}
        async with self.with_connection() as conn:
            results = await conn.fetch(
                dedent(f"""
                SELECT worker_id, stats FROM {self.stats_table}
                WHERE $1 <= expire_at
                """),
                int(seconds(now())),
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
                    SELECT count(*) FROM {self.jobs_table}
                    WHERE status = 'queued'
                      AND queue = $1
                      AND $2 >= scheduled
                    """),
                    self.name,
                    int(seconds(now())),
                )
            elif kind == "active":
                result = await conn.fetchval(
                    dedent(f"""
                    SELECT count(*) FROM {self.jobs_table}
                    WHERE status = 'active'
                      AND queue = $1
                    """),
                    self.name,
                )
            elif kind == "incomplete":
                result = await conn.fetchval(
                    dedent(f"""
                    SELECT count(*) FROM {self.jobs_table}
                    WHERE status IN ('new', 'deferred', 'queued', 'active')
                      AND queue = $1
                    """),
                    self.name,
                )
            else:
                raise ValueError(f"Can't count unknown type {kind}")

            return result

    async def schedule(self, lock: int = 1) -> list[str]:
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
                return []
            self._has_sweep_lock = True

        async with self.with_connection() as conn, conn.transaction():
            expired_at = int(seconds(now()))
            await conn.execute(
                dedent(f"""
                DELETE FROM {self.jobs_table}
                WHERE queue = $1
                AND status IN ('aborted', 'complete', 'failed')
                AND $2 >= expire_at
                """),
                self.name,
                expired_at,
            )
            await conn.execute(
                dedent(f"""
                DELETE FROM {self.stats_table}
                WHERE $1 >= expire_at;
                """),
                expired_at,
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
            key, job_bytes, objid, _status = row.values()
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

    async def notify(
        self, job: Job, connection: PoolConnectionProxy | None = None
    ) -> None:
        await self._notify(job.key, job.status, connection)

    async def update(
        self,
        job: Job,
        connection: PoolConnectionProxy | None = None,
        expire_at: float | None = -1,
        **kwargs: Any,
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
            cursor = await conn.cursor(
                f"SELECT job FROM {self.jobs_table} WHERE key = $1", job_key
            )
            record = await cursor.fetchrow()
            return self.deserialize(record.get("job")) if record else None

    async def jobs(self, job_keys: Iterable[str]) -> List[Job | None]:
        keys = list(job_keys)
        results = {}
        async with self.with_connection() as conn, conn.transaction():
            async for record in conn.cursor(
                f"SELECT key, job FROM {self.jobs_table} WHERE key = ANY($1)", keys
            ):
                results[record.get("key")] = record.get("job")
        return [self.deserialize(results.get(key)) for key in keys]

    async def iter_jobs(
        self,
        statuses: List[Status] = list(Status),
        batch_size: int = 100,
    ) -> AsyncIterator[Job]:
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
            status = await self.get_job_status(
                job.key, for_update=True, connection=conn
            )
            if status == Status.QUEUED:
                await self.finish(job, Status.ABORTED, error=error, connection=conn)
            else:
                await self.update(
                    job, status=Status.ABORTING, error=error, connection=conn
                )

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
                    async for payload in self._listener.listen(
                        ENQUEUE, DEQUEUE, timeout=timeout
                    ):
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
                        AND $2 >= scheduled
                      ORDER BY scheduled
                      LIMIT $3
                      FOR UPDATE SKIP LOCKED
                    )
                    UPDATE {self.jobs_table} SET status = 'active'
                    FROM locked_job
                    WHERE {self.jobs_table}.key = locked_job.key
                      AND pg_try_advisory_lock({self.job_lock_keyspace}, locked_job.lock_key)
                    RETURNING job
                    """),
                    self.name,
                    int(seconds(now())),
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
                INSERT INTO {self.jobs_table} (key, job, queue, status, scheduled)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (key) DO UPDATE
                SET
                  job = $2,
                  queue = $3,
                  status = $4,
                  scheduled = $5,
                  expire_at = null
                WHERE
                  {self.jobs_table}.status IN ('aborted', 'complete', 'failed')
                  AND $5 > {self.jobs_table}.scheduled
                RETURNING 1
                """,
                job.key,
                self.serialize(job),
                self.name,
                job.status,
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
            scheduled = job.scheduled or int(seconds(now()))

        await self.update(job, scheduled=int(scheduled), expire_at=None)

    async def _finish(
        self,
        job: Job,
        status: Status,
        *,
        result: Any = None,
        error: str | None = None,
        connection: PoolConnectionProxy | None = None,
    ) -> None:
        key = job.key

        async with self.with_connection(connection) as conn:
            if job.ttl >= 0:
                expire_at = int(seconds(now())) + job.ttl if job.ttl > 0 else None
                await self.update(
                    job, status=status, expire_at=expire_at, connection=conn
                )
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
        data: Any | None = None,
        connection: PoolConnectionProxy | None = None,
    ) -> None:
        payload = {"key": key}

        if data is not None:
            payload["data"] = data

        async with self.with_connection(connection) as conn:
            await conn.execute(f"NOTIFY \"{self._channel}\", '{json.dumps(payload)}'")

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

    @asynccontextmanager
    async def _get_dequeue_conn(self) -> AsyncGenerator[PoolConnectionProxy]:
        async with self._connection_lock:
            if self._dequeue_conn:
                try:
                    await self._dequeue_conn.execute("SELECT 1")
                except (ConnectionDoesNotExistError, InterfaceError):
                    await self.pool.release(self._dequeue_conn)
                    self._dequeue_conn = await self.pool.acquire()
            else:
                self._dequeue_conn = await self.pool.acquire()

            yield self._dequeue_conn


    @asynccontextmanager
    async def _nullcontext(self, enter_result: ContextT) -> AsyncGenerator[ContextT]:
        """Async version of contextlib.nullcontext

        Async support has been added to contextlib.nullcontext in Python 3.10.
        """
        yield enter_result


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
        payload: Any,
    ) -> None:
        payload_data = json.loads(payload)
        self.publish(payload_data["key"], payload_data)
