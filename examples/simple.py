import asyncio

from saq import Job, Queue


async def test(ctx):
    await asyncio.sleep(1.1)
    return {"a": 1}


queue = Queue.from_url("redis://localhost:6380")

settings = {
    "queue": queue,
    "functions": [test],
    "concurrency": 1,
}

async def enqueue():
    count = 0
    while count < 1:
        count += 1
        await Job("test", queue=queue).enqueue()


if __name__ == "__main__":
    asyncio.run(enqueue())
