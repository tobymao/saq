import asyncio
import logging
import time

from saq import CronJob, Queue, monitored_job

logger = logging.getLogger("saq")
logger.setLevel(logging.DEBUG)


@monitored_job
async def monitored_sleeper(ctx):
    print("executing monitored sleeper job.")

    await asyncio.sleep(30)
    return {"a": 6}


async def sleeper(ctx):
    await asyncio.sleep(30)
    print("executing regular sleeper job.  I should fail due to missed heartbeat")

    return {"a": 6}


@monitored_job
async def monitored_cron_job(ctx):
    print("excuting cron job")
    await asyncio.sleep(6)


async def cron_job(ctx):
    print("excuting regular cron job.  I should fail due to missed heartbeat")
    await asyncio.sleep(6)


settings = {
    "functions": [monitored_sleeper, monitored_cron_job, sleeper, cron_job],
    "concurrency": 100,
    "timers": {"sweep": 5},
    "cron_jobs": [
        CronJob(monitored_cron_job, cron="* * * * * */20", heartbeat=2, timeout=None),
        CronJob(cron_job, cron="* * * * * */20", heartbeat=2, timeout=None),
    ],
}


async def enqueue(func, **kwargs):
    queue = Queue.from_url("redis://localhost")
    result = await queue.apply(
        func,
        heartbeat=3,
    )
    print(result)


if __name__ == "__main__":
    now = time.time()
    asyncio.run(enqueue("monitored_sleeper"))
    asyncio.run(enqueue("sleeper"))

    print(time.time() - now)
