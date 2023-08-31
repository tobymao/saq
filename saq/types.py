"""
Types
"""
from __future__ import annotations

import typing as t

if t.TYPE_CHECKING:
    from asyncio import Task

    from saq.job import Job, Status
    from saq.worker import Worker
    from saq.queue import Queue
    from typing_extensions import Required


class Context(t.TypedDict, total=False):
    """
    Task context
    """

    worker: Required[Worker]
    job: Job
    queue: Queue
    sleep: int


class JobTaskContext(t.TypedDict, total=False):
    """
    Jobs Task Context
    """

    task: Task[t.Any]
    aborted: bool


class QueueInfo(t.TypedDict):
    """
    Queue Info
    """

    workers: dict[str, dict[str, t.Any]]
    name: str
    queued: int
    active: int
    scheduled: int
    jobs: list[dict[str, t.Any]]


class QueueStats(t.TypedDict):
    """
    Queue Stats
    """

    complete: int
    failed: int
    retried: int
    aborted: int
    uptime: int


class TimersDict(t.TypedDict):
    """
    Timers Dictionary
    """

    schedule: int
    stats: int
    sweep: int
    abort: int


class PartialTimersDict(TimersDict, total=False):
    """
    For argument to `Worker`, all keys are not required
    """


BeforeEnqueueType = t.Callable[["Job"], t.Awaitable[t.Any]]
CountKind = t.Literal["queued", "active", "incomplete"]
DumpType = t.Callable[[t.Dict], str]
DurationKind = t.Literal["process", "start", "total", "running"]
Function = t.Callable[..., t.Any]
ListenCallback = t.Callable[[str, "Status"], t.Any]
LoadType = t.Callable[[t.Union[bytes, str]], t.Any]
ReceivesContext = t.Callable[[Context], t.Awaitable[t.Any]]
VersionTuple = t.Tuple[int, ...]
