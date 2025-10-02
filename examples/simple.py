import asyncio
import random
import time

from saq import CronJob, Queue
from saq.types import Context, SettingsDict

CtxType = Context


async def sleeper(ctx: CtxType, *, a):
    await asyncio.sleep(a)
    return {"a": a}


async def adder(ctx: CtxType, *, a, b):
    await asyncio.sleep(1)
    return a + b


async def cron_job(ctx):
    print("executing cron job")


queue = Queue.from_url("postgres://postgres@localhost")

settings = SettingsDict[CtxType](
    queue=queue,
    functions=[sleeper, adder],
    concurrency=100,
    cron_jobs=[CronJob(cron_job, cron="* * * * * */5")],
)


async def enqueue(func, **kwargs):
    await queue.connect()

    for _ in range(10000):
        await queue.enqueue(func, **{k: v() for k, v in kwargs.items()})


if __name__ == "__main__":
    now = time.time()
    asyncio.run(enqueue("sleeper", a=random.random))
    asyncio.run(enqueue("adder", a=random.random, b=random.random))
    print(time.time() - now)
