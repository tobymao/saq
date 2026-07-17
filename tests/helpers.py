import asyncio
import time
import typing as t

import psycopg

from saq.job import Job, Status
from saq.queue import Queue
from saq.queue.postgres import PostgresQueue
from saq.queue.redis import RedisQueue

POSTGRES_TEST_SCHEMA = "test_saq"


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


async def wait_for_job(job: Job, predicate: t.Callable[[Job], bool], timeout: float = 10) -> None:
    """Refresh the job until the predicate holds or the deadline expires.

    Callers still assert on the job afterwards, so a timeout surfaces the actual
    state instead of hiding it. Use this instead of a fixed sleep before asserting
    job state: fixed sleeps are the main source of flakes on loaded CI runners.
    """
    deadline = time.monotonic() + timeout
    while not predicate(job) and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
        await job.refresh()


async def wait_for_status(job: Job, *statuses: Status, timeout: float = 10) -> None:
    """Refresh the job until its status is one of statuses or the deadline expires."""
    await wait_for_job(job, lambda j: j.status in statuses, timeout=timeout)


async def cleanup_queue(queue: Queue) -> None:
    if isinstance(queue, RedisQueue):
        await queue.redis.flushdb()
    await queue.disconnect()


async def setup_postgres() -> None:
    async with await psycopg.AsyncConnection.connect(
        "postgres://postgres@localhost", autocommit=True
    ) as conn:
        # kill connections leaked by previous tests so they can't block the drop
        # or hold the init_db advisory lock
        await conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
            " WHERE pid <> pg_backend_pid() AND datname = current_database()"
        )
        await conn.execute(f"DROP SCHEMA IF EXISTS {POSTGRES_TEST_SCHEMA} CASCADE")
        await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {POSTGRES_TEST_SCHEMA}")


async def teardown_postgres() -> None:
    async with await psycopg.AsyncConnection.connect(
        "postgres://postgres@localhost", autocommit=True
    ) as conn:
        await conn.execute(f"DROP SCHEMA {POSTGRES_TEST_SCHEMA} CASCADE")
