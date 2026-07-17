"""
Utils
"""

from __future__ import annotations

import asyncio
import time
import typing as t
import uuid
from random import random

if t.TYPE_CHECKING:
    from collections.abc import Iterable


def now() -> int:
    """Gets current time in milliseconds since epoch"""
    return int(time.time() * 1000)


def now_seconds() -> float:
    return time.time()


def uuid1() -> str:
    """Generates a string representation of a UUID1"""
    return str(uuid.uuid1())


def millis(s: float) -> float:
    """Converts from seconds to milliseconds"""
    return s * 1000


def seconds(ms: float) -> float:
    """Converts from milliseconds to seconds"""
    return ms / 1000


def exponential_backoff(
    attempts: int,
    base_delay: float,
    max_delay: float | None = None,
    jitter: bool = True,
) -> float:
    """
    Get the next delay for retries in exponential backoff.

    Args:
        attempts: Number of attempts so far
        base_delay: Base delay, in seconds
        max_delay: Max delay, in seconds. If None (default), there is no max.
        jitter: If True, add a random jitter to the delay

    Returns:
        Delay in seconds
    """
    if max_delay is None:
        max_delay = float("inf")
    backoff = min(max_delay, base_delay * 2 ** max(attempts - 1, 0))
    if jitter:
        backoff = backoff * random()
    return backoff


async def wait_for(awaitable: t.Awaitable, timeout: float | None) -> t.Any:
    """Like asyncio.wait_for, but robust to swallowed cancellations.

    Before Python 3.12, asyncio.wait_for cancels the task once when the timeout
    fires and then waits forever for it to finish. That cancellation is lost if
    it races a completing future inside a nested wait_for (bpo-42130) - e.g. the
    waits inside psycopg's connection pool - deadlocking a task that polls in a
    loop, like Queue.listen. Cancel repeatedly until the task actually finishes.
    """
    task = asyncio.ensure_future(awaitable)
    try:
        done, _ = await asyncio.wait({task}, timeout=timeout)
        if done:
            return task.result()
        raise asyncio.TimeoutError
    finally:
        while not task.done():
            task.cancel()
            await asyncio.wait({task}, timeout=0.1)


async def cancel_tasks(
    tasks: Iterable[asyncio.Task],
    timeout: float | None = 1.0,
) -> bool:
    """Cancel tasks and wait for all of them to finish"""
    tasks = list(tasks)
    for task in tasks:
        task.cancel()

    if tasks:
        # asyncio.wait instead of wait_for(gather(...)): wait_for waits for the
        # gather to finish even after its timeout, hanging on a stuck task.
        await asyncio.wait(tasks, timeout=timeout)
    for task in tasks:
        if task.done() and not task.cancelled():
            task.exception()
    return all(task.done() for task in tasks)
