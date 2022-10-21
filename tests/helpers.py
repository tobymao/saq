from saq.queue import Queue


def create_queue(**kwargs) -> Queue:
    return Queue.from_url("redis://localhost:6379", **kwargs)


async def cleanup_queue(queue: Queue) -> None:
    await queue.redis.flushdb()
    await queue.disconnect()
