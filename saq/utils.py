from __future__ import annotations

import time
import uuid
import typing as t
from random import random


def now() -> int:
    return int(time.time() * 1000)


def uuid1() -> str:
    return str(uuid.uuid1())


def millis(s: float) -> float:
    return s * 1000


def seconds(ms: float) -> float:
    return ms / 1000


def exponential_backoff(
    attempts: int,
    base_delay: float,
    max_delay: t.Optional[float] = None,
    jitter: bool = True,
) -> float:
    """
    Get the next delay for retries in exponential backoff.

    attempts: Number of attempts so far
    base_delay: Base delay, in seconds
    max_delay: Max delay, in seconds. If None (default), there is no max.
    jitter: If True, add a random jitter to the delay
    """
    if max_delay is None:
        max_delay = float("inf")
    backoff = min(max_delay, base_delay * 2 ** max(attempts - 1, 0))
    if jitter:
        backoff = backoff * random()
    return backoff
