import asyncio
import logging

from saq import Job, Queue


logging.basicConfig()
logger = logging.getLogger("saq")
logger.setLevel(logging.INFO)


async def test(ctx):
    await asyncio.sleep(1.1)
    return {"a": 1}


queue = Queue.from_url("redis://localhost:6380")

settings = {
    "queue": queue,
    "functions": [test],
    "concurrency": 5,
}

async def enqueue():
    count = 0
    while count < 1:
        count += 1
        await Job("test",  queue=queue).enqueue()


if __name__ == "__main__":
    asyncio.run(enqueue())
