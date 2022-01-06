import asyncio
import random
import time

from saq import Queue


async def sleeper(ctx, *, a):
    await asyncio.sleep(1)
    return {"a": a}


async def adder(ctx, *, a, b):
    await asyncio.sleep(1)
    return a + b


settings = {
    "functions": [sleeper, adder],
    "concurrency": 10,
}

async def enqueue(func, **kwargs):
    queue = Queue.from_url("redis://localhost")
    for _ in range(10000):
        await queue.enqueue(func, **kwargs)


if __name__ == "__main__":
    now = time.time()
    asyncio.run(enqueue("sleeper", a=random.randint(0, 100)))
    asyncio.run(enqueue("adder", a=random.randint(0, 100), b=random.randint(0, 100)))
    print(time.time() - now)
