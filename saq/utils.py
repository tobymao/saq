from __future__ import annotations

import typing as t

import time
import uuid
from random import random

from .types import Function


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
    max_delay: float | None = None,
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


def normalize_function(
    function: t.Union[Function, str]
) -> t.Callable[..., t.Coroutine]:
    """
    Taken from pydantic.utils.
    """
    from importlib import import_module

    if not isinstance(function, str):
        return function

    module_path, class_name = "", ""
    try:
        if function.find(".") >= 0:
            module_path, class_name = function.strip(" ").rsplit(".", 1)
        else:
            raise ImportError(
                f'String specified Function "{function}" must be specified as "full.path.to.function"'
            )
    except ValueError as e:
        raise ImportError(f'"{function}" doesn\'t look like a module path') from e

    module = import_module(module_path)
    try:
        return getattr(module, class_name)
    except AttributeError as e:
        raise ImportError(
            f'Module "{module_path}" does not define a "{class_name}" attribute'
        ) from e
