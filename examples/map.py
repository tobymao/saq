import asyncio

import aioredis
from saq import Queue


def get_queue():
    # Spawning 10,000 jobs at once can easily exceed connection limits.
    # Using a effectively BlockingConnectionPool throttles the throughput of enqueue jobs.
    pool = aioredis.BlockingConnectionPool.from_url("redis://localhost")
    return Queue(
        redis=aioredis.Redis(
            connection_pool=pool
        )
    )


async def square(ctx, *, a):
    return a**2


async def sum_of_squares(ctx, *, n):
    queue = get_queue()
    async with queue.batch():
        squares = await queue.map(
            square.__name__,
            [{"a": i} for i in range(n)],
        )
    return sum(squares)


settings = {
    "functions": [square, sum_of_squares],
    "concurrency": 100,
}


async def enqueue():
    queue = get_queue()
    result = await queue.apply(sum_of_squares.__name__, n=10000, timeout=60)
    print(result)


if __name__ == "__main__":
    asyncio.run(enqueue())
