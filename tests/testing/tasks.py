from saq import Context, Job, Queue


async def add(  # pylint: disable=unused-argument
    ctx: Context, *, val1: int, val2: int
) -> int:
    return val1 + val2


async def boom(ctx: Context, *, value: str) -> int:  # pylint: disable=unused-argument
    raise ValueError(value)


queue = Queue.from_url("redis://localhost")
settings = {
    "queue": queue,
    "functions": [add, boom],
}


async def applies_a_job() -> int:
    return await queue.apply("add", val1=7, val2=11)


async def enqueues_a_job() -> Job:
    return await queue.enqueue("add", val1=7, val2=11)  # type: ignore


async def enqueues_a_job_that_retries() -> Job:
    return await queue.enqueue("boom", value="For retrying", retries=10)  # type: ignore
