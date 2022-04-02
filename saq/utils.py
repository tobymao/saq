import time
import uuid
from random import random


def now():
    return int(time.time() * 1000)


def uuid1():
    return str(uuid.uuid1())


def millis(s):
    return s * 1000


def seconds(ms):
    return ms / 1000


def exponential_backoff(
    attempts,
    base_delay,
    max_delay=None,
    jitter=True,
):
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
