from __future__ import annotations

import typing as t

if t.TYPE_CHECKING:
    from saq.job import Job, Status

BeforeEnqueueType = t.Callable[["Job"], t.Awaitable[t.Any]]
Context = t.Dict[str, t.Any]
CountKind = t.Literal["queued", "active", "incomplete"]
DumpType = t.Callable[[t.Dict], str]
DurationKind = t.Literal["process", "start", "total", "running"]
Function = t.Callable
ListenCallback = t.Callable[[str, "Status"], t.Any]
LoadType = t.Callable[[t.Union[bytes, str]], t.Any]
ReceivesContext = t.Callable[[Context], t.Awaitable[t.Any]]
VersionTuple = t.Tuple[int, ...]


class QueueInfo(t.TypedDict):
    workers: dict
    name: str
    queued: int
    active: int
    scheduled: int
    jobs: list[dict]


class QueueStats(t.TypedDict):
    complete: int
    failed: int
    retried: int
    aborted: int
    uptime: int


class TimersDict(t.TypedDict, total=False):
    schedule: int
    stats: int
    sweep: int
    abort: int
