from __future__ import annotations

import typing as t
from saq.compat import Literal, TypedDict

if t.TYPE_CHECKING:
    from saq.job import Job, Status

BeforeEnqueueType = t.Callable[["Job"], t.Awaitable[t.Any]]
Context = t.Dict[str, t.Any]
CountKind = Literal["queued", "active", "incomplete"]
DumpType = t.Callable[[t.Dict], str]
DurationKind = Literal["process", "start", "total", "running"]
Function = t.Callable
ListenCallback = t.Callable[[str, "Status"], t.Any]
LoadType = t.Callable[[t.Union[bytes, str]], t.Any]
ReceivesContext = t.Callable[[Context], t.Awaitable[t.Any]]
VersionTuple = t.Tuple[int, ...]


class QueueInfo(TypedDict):
    workers: dict
    name: str
    queued: int
    active: int
    scheduled: int
    jobs: list[dict]


class QueueStats(TypedDict):
    complete: int
    failed: int
    retried: int
    aborted: int
    uptime: int


class TimersDict(TypedDict, total=False):
    schedule: int
    stats: int
    sweep: int
    abort: int
