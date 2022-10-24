from __future__ import annotations

import typing as t
from saq.compat import Literal, TypedDict

if t.TYPE_CHECKING:
    from saq.job import Job

BeforeEnqueueType = t.Callable[[Job], t.Awaitable[t.Any]]
DumpType = t.Callable[[t.Dict], str]
LoadType = t.Callable[[t.Union[bytes, str]], t.Any]
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
