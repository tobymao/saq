import asyncio
import time

from saq import Job, Queue


async def test(ctx):
    await asyncio.sleep(1)
    return {"a": 1}


queue = Queue.from_url("redis://localhost")

settings = {
    "queue": queue,
    "functions": [test],
    "concurrency": 10,
}

async def enqueue():
    for _ in range(10000):
        await queue.enqueue("test")


if __name__ == "__main__":
    now = time.time()
    asyncio.run(enqueue())
    print(time.time() - now)
