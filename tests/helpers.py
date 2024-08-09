import typing as t

from saq.queue import Queue
from saq.queue.redis import RedisQueue


def create_queue(**kwargs: t.Any) -> RedisQueue:
    queue = t.cast(RedisQueue, Queue.from_url("redis://localhost:6379", **kwargs))
    return queue


async def cleanup_queue(queue: RedisQueue) -> None:
    await queue.redis.flushdb()
    await queue.disconnect()
