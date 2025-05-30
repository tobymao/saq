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


async def cancel_tasks(tasks: Iterable[asyncio.Task], timeout: float = 1.0) -> None:
    """Cancel tasks and wait for all of them to finish"""
    for task in tasks:
        task.cancel()

    try:
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass
