import time
import uuid
from random import random
from typing import Optional


def now():
    return int(time.time() * 1000)


def uuid1():
    return str(uuid.uuid1())


def millis(s):
    return s * 1000


def seconds(ms):
    return ms / 1000


def exponential_backoff(
    attempts: int,
    base_delay: float,
    max_delay: Optional[float] = None,
    jitter: bool = True,
) -> float:
    """
    Get the next delay for retries in exponential backoff.

    attempts: Number of attempts so far
    base_delay: Base delay, in seconds
    max_delay: Max delay, in seconds
    jitter: If True, add a random jitter to the delay
    """
    if max_delay is None:
        max_delay = float("inf")
    retries = max(attempts - 1, 0)
    backoff = min(max_delay, base_delay * 2**retries)
    if jitter:
        backoff = backoff * random()
    return backoff
