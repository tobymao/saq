import typing as t

import psycopg

from saq.queue import Queue
from saq.queue.postgres import PostgresQueue
from saq.queue.redis import RedisQueue


async def create_redis_queue(**kwargs: t.Any) -> RedisQueue:
    queue = t.cast(RedisQueue, Queue.from_url("redis://localhost:6379", **kwargs))
    await queue.connect()
    return queue


async def create_postgres_queue(**kwargs: t.Any) -> PostgresQueue:
    with psycopg.connect("postgres://postgres@localhost", autocommit=True) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS test_saq")

    queue = t.cast(
        PostgresQueue,
        Queue.from_url("postgres://postgres@localhost?options=--search_path%3Dtest_saq", **kwargs),
    )
    await queue.connect()
    return queue


async def cleanup_queue(queue: Queue) -> None:
    await queue.disconnect()
    if isinstance(queue, RedisQueue):
        await queue.redis.flushdb()
    elif isinstance(queue, PostgresQueue):
        with psycopg.connect("postgres://postgres@localhost", autocommit=True) as conn:
            conn.execute("DROP SCHEMA test_saq CASCADE")
