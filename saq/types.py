from __future__ import annotations

import typing as t

if t.TYPE_CHECKING:
    from saq.job import Job

BeforeEnqueueType = t.Callable[[Job], t.Awaitable[t.Any]]
DumpType = t.Callable[[t.Dict], str]
LoadType = t.Callable[[t.Union[bytes, str]], t.Any]
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
