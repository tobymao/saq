import asyncio

from saq import Queue


queue = Queue.from_url("redis://localhost")


async def square(ctx, *, a):
    return a**2


async def sum_of_squares(ctx, *, n):
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
    result = await queue.apply(sum_of_squares.__name__, n=10000)
    print(result)


if __name__ == "__main__":
    asyncio.run(enqueue())
