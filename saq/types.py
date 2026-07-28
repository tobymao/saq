"""
Types
"""

from __future__ import annotations

import typing as t
from collections.abc import Collection
from typing import Generic

import typing_extensions as te
from typing_extensions import Required, TypedDict

if t.TYPE_CHECKING:
    from asyncio import Task

    from saq.job import CronJob, Job, Status
    from saq.queue import Queue
    from saq.worker import Worker


class Context(TypedDict, total=False):
    """
    Task context.

    Extra context fields are allowed.
    """

    worker: Required[Worker]
    "Worker currently executing the task"
    job: Job
    "Job() instance of the task"
    queue: Queue
    "Queue the task is running on"
    exception: Exception | None
    "Exception raised by the task if any"


class JobTaskContext(TypedDict, total=True):
    """
    Jobs Task Context
    """

    task: Task[t.Any]
    "asyncio Task of the Job"
    aborted: str | None
    "If this task has been aborted, this is the reason"


class WorkerInfo(TypedDict):
    """
    Worker Info
    """

    queue_key: str | None
    stats: WorkerStats | None
    metadata: dict[str, t.Any] | None


class QueueInfo(TypedDict):
    """
    Queue Info
    """

    workers: dict[str, WorkerInfo]
    "Worker information"
    name: str
    "Queue name"
    queued: int
    "Number of jobs currently in the queue"
    active: int
    "Number of jobs currently active"
    scheduled: int
    jobs: list[dict[str, t.Any]]
    "A truncated list containing the jobs that are scheduled to execute soonest"


class WorkerStats(TypedDict):
    """
    Worker Stats
    """

    complete: int
    "Number of complete tasks"
    failed: int
    "Number of failed tasks"
    retried: int
    "Number of retries"
    aborted: int
    "Number of aborted tasks"
    uptime: int
    "Queue uptime in milliseconds"


class TimersDict(TypedDict):
    """
    Timers Dictionary
    """

    schedule: int
    "How often we poll to schedule jobs in seconds (default 1)"
    worker_info: int
    "How often to update worker info, stats and metadata in seconds (default 10)"
    sweep: int
    "How often to clean up stuck jobs in seconds (default 60)"
    abort: int
    "How often to check if a job is aborted in seconds (default 1)"


class PartialTimersDict(TimersDict, total=False):
    """
    For argument to `Worker`, all keys are not required
    """


CtxType = t.TypeVar("CtxType", bound=Context)


class SettingsDict(TypedDict, Generic[CtxType], total=False):
    """
    Settings
    """

    queue: Queue
    functions: Required[FunctionsType[CtxType]]
    concurrency: int
    cron_jobs: Collection[CronJob]
    startup: ReceivesContext[CtxType]
    shutdown: ReceivesContext[CtxType]
    before_process: ReceivesContext[CtxType]
    after_process: ReceivesContext[CtxType]
    timers: PartialTimersDict
    dequeue_timeout: float


P = te.ParamSpec("P")

BeforeEnqueueType = t.Callable[["Job"], t.Awaitable[t.Any]]
CountKind = t.Literal["queued", "active", "incomplete"]
DumpType = t.Callable[[t.Mapping[t.Any, t.Any]], bytes | str]
DurationKind = t.Literal["process", "start", "total", "running"]
Function = t.Callable[te.Concatenate[CtxType, ...], t.Any]
FunctionsType: te.TypeAlias = Collection[Function[CtxType] | tuple[str, Function[CtxType]]]
ReceivesContext = t.Callable[[CtxType], t.Any]
LifecycleFunctionsType = ReceivesContext[CtxType] | Collection[ReceivesContext[CtxType]]
ListenCallback = t.Callable[[str, "Status"], t.Any]
LoadType = t.Callable[[bytes | str], t.Any]
VersionTuple = tuple[int, ...]
