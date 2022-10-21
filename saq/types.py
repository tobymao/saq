from __future__ import annotations

from typing import TYPE_CHECKING, TypedDict

if TYPE_CHECKING:
    from typing import Any, Awaitable, Callable, Dict, Tuple, Union
    from typing_extensions import TypeAlias
    from .job import Job

BeforeEnqueueType: TypeAlias = "Callable[[Job], Awaitable[Any]]"
DumpType: TypeAlias = "Callable[[Dict], str]"
LoadType: TypeAlias = "Callable[[Union[bytes, str]], Any]"
VersionTuple: TypeAlias = "Tuple[int, ...]"


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
