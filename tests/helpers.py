import asyncio
import typing as t

import asyncpg

from saq.queue import Queue
from saq.queue.postgres import PostgresQueue
from saq.queue.redis import RedisQueue

POSTGRES_TEST_SCHEMA = "test_saq"


async def create_redis_queue(**kwargs: t.Any) -> RedisQueue:
    queue = t.cast(RedisQueue, Queue.from_url("redis://localhost:6379", **kwargs))
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


async def cleanup_queue(queue: Queue) -> None:
    if isinstance(queue, RedisQueue):
        await queue.redis.flushdb()
    await queue.disconnect()


async def setup_postgres() -> None:
    async with asyncpg.create_pool("postgres://postgres@localhost") as pool:
        await pool.execute(f"DROP SCHEMA IF EXISTS {POSTGRES_TEST_SCHEMA} CASCADE")
        await pool.execute(f"CREATE SCHEMA IF NOT EXISTS {POSTGRES_TEST_SCHEMA}")


async def teardown_postgres() -> None:
    async with asyncpg.create_pool("postgres://postgres@localhost") as pool:
        await pool.execute(f"DROP SCHEMA {POSTGRES_TEST_SCHEMA} CASCADE")
