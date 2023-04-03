import asyncio
import random
import time

from saq import CronJob, Queue


async def sleeper(ctx, *, a):
    await asyncio.sleep(a)
    print("executing sleeper")
    return {"a": a}


async def adder(ctx, *, a, b):
    print("executing adder")
    await asyncio.sleep(1)
    return a + b


async def cron_job(ctx):
    print("excuting cron job")


settings = {
    "functions": [sleeper, "a.b.c.foo.something_rad"],
    "concurrency": 100,
    "cron_jobs": [CronJob(cron_job, cron="* * * * * */5")],
}


async def enqueue_me(func, **kwargs):
    queue = Queue.from_url("redis://localhost")
    for _ in range(1):
        await queue.enqueue(func, **{k: v() for k, v in kwargs.items()})


if __name__ == "__main__":
    now = time.time()
    asyncio.run(enqueue_me(sleeper, a=random.random))
    asyncio.run(enqueue_me("a.b.c.foo.something_rad", a=random.random, b=random.random))
    asyncio.run(enqueue_me("simple.sleeper", a=random.random))
    print(time.time() - now)
