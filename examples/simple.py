import asyncio
import time

from saq import Job, Queue


async def test(ctx, a, b, c):
    await asyncio.sleep(5)
    return {"a": 1}


queue = Queue.from_url("redis://localhost")

settings = {
    "queue": queue,
    "functions": [test],
    "concurrency": 10,
}

async def enqueue():
    sem = asyncio.Semaphore(100)
    async def sem_task(task):
        async with sem:
            return await task
    tasks = [
        asyncio.create_task(sem_task(queue.enqueue("test", a=1, b=2, c=3)))
        for _ in range(10000)
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    now = time.time()
    asyncio.run(enqueue())
    print(time.time() - now)
