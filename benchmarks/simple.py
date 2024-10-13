import asyncio
import sys
import time

from funcs import *


SEM = asyncio.Semaphore(20)
N = 1000


async def sem_task(task):
    async with SEM:
        return await task


async def bench_arq():
    from arq import create_pool
    from arq.connections import RedisSettings
    from arq.worker import Worker

    async def enqueue(func):
        await asyncio.gather(
            *[asyncio.create_task(sem_task(redis.enqueue_job(func))) for _ in range(N)]
        )

    redis = await create_pool(RedisSettings())
    worker = Worker(functions=[noop, sleeper], max_jobs=10, burst=True)
    worker.loop = asyncio.get_event_loop()

    now = time.time()
    await enqueue("noop")
    print(f"ARQ enqueue {N} {time.time() - now}")

    now = time.time()
    await worker.main()
    print(f"ARQ process {N} noop {time.time() - now}")

    await enqueue("sleeper")
    now = time.time()
    await worker.main()
    print(f"ARQ process {N} sleep {time.time() - now}")


async def bench_saq(url: str):
    from saq import Queue, Worker

    async def enqueue(func):
        await asyncio.gather(*[asyncio.create_task(queue.enqueue(func)) for _ in range(N)])

    queue = Queue.from_url(url)
    worker = Worker(queue=queue, functions=[noop, sleeper], concurrency=10)
    await queue.connect()

    now = time.time()
    await enqueue("noop")
    print(f"SAQ enqueue {N} {time.time() - now}")

    now = time.time()
    task = asyncio.create_task(worker.start())

    while await queue.count("incomplete"):
        await asyncio.sleep(0.1)
    print(f"SAQ process {N} noop {time.time() - now}")

    await enqueue("sleeper")
    now = time.time()
    while await queue.count("incomplete"):
        await asyncio.sleep(0.1)
    print(f"SAQ process {N} sleep {time.time() - now}")
    await worker.stop()
    await queue.disconnect()


def bench_rq():
    from rq import Connection, Queue, Worker

    with Connection() as connection:
        queue = Queue(connection=connection)
        worker = Worker("default", log_job_description=False)
        worker.log_result_lifespan = False

        def enqueue(func):
            for _ in range(N):
                queue.enqueue(func)

        now = time.time()
        enqueue(sync_noop)
        print(f"RQ enqueue {N} {time.time() - now}")
        worker.work(burst=True)
        print(f"RQ process {N} noop {time.time() - now}")

        enqueue(sync_sleeper)
        worker.work(burst=True)
        print(f"RQ process {N} sleep {time.time() - now}")


async def main():
    lib = sys.argv[1]

    if lib == "arq":
        await bench_arq()
    elif lib == "rq":
        bench_rq()
    elif lib == "saq_pg":
        await bench_saq("postgres://postgres@localhost")
    else:
        await bench_saq("redis://localhost")


if __name__ == "__main__":
    asyncio.run(main())
