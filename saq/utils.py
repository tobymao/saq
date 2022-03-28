import time
import uuid
import inspect


def now():
    return int(time.time() * 1000)


def uuid1():
    return str(uuid.uuid1())


def millis(s):
    return s * 1000


def seconds(ms):
    return ms / 1000


def ensure_async_generator(func):
    if inspect.isasyncgenfunction(func):
        return func

    async def generator(job):
        await func(job)
        yield

    return generator


async def run_iterator_to_first_yield(async_iter):
    await async_iter.__anext__()


async def run_iterator_to_completion(async_iter):
    async for _ in async_iter:
        pass
