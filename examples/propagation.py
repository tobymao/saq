import asyncio
import contextvars
import logging

from saq import Queue
from saq.utils import uuid1

logger = logging.getLogger(__name__)

correlation_id_ctx = contextvars.ContextVar("correlation_id_ctx")

queue = Queue.from_url("redis://localhost")


async def recurse(ctx, *, n):
    logger.info(n)
    if n == 0:
        return
    await queue.apply("recurse", n=n - 1)


async def before_process(ctx):
    correlation_id = ctx["job"].meta.get("correlation_id")
    ctx["reset_token"] = correlation_id_ctx.set(correlation_id)


async def after_process(ctx):
    correlation_id_ctx.reset(ctx["reset_token"])


async def before_enqueue(job):
    job.meta["correlation_id"] = correlation_id_ctx.get(None) or uuid1()


queue.register_before_enqueue(before_enqueue)

settings = {
    "queue": queue,
    "functions": [recurse],
    "concurrency": 100,
    "before_process": before_process,
    "after_process": after_process,
}


class LoggingContextFilter(logging.Filter):
    def filter(self, record):
        record.correlation_id = correlation_id_ctx.get(None)
        return True


handler = logging.StreamHandler()
handler.addFilter(LoggingContextFilter())
handler.setFormatter(logging.Formatter("%(correlation_id)s - %(message)s"))
handler.setLevel(logging.INFO)
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.handlers = [handler]


async def enqueue():
    await queue.apply("recurse", n=3)


if __name__ == "__main__":
    asyncio.run(enqueue())
