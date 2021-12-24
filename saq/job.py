import dataclasses
import enum
import typing

from saq.utils import now, seconds, uuid1


class Status(str, enum.Enum):
    NEW = "new"
    DEFERRED = "deferred"
    QUEUED = "queued"
    ACTIVE = "active"
    ABORTED = "aborted"
    FAILED = "failed"
    COMPLETE = "complete"


@dataclasses.dataclass
class Job:
    function: str
    kwargs: typing.Optional[dict] = None
    queue: typing.Optional["Queue"] = None
    job_id: str = dataclasses.field(default_factory=uuid1)
    timeout: int = 10
    heartbeat: int = 0
    retries: int = 1
    ttl: int = 60
    scheduled: int = 0
    attempts: int = 0
    completed: int = 0
    enqueued: int = 0
    started: int = 0
    touched: int = 0
    result: typing.Optional[dict] = None
    error: typing.Optional[str] = None
    status: Status = Status.NEW

    def __repr__(self):
        kwargs = ", ".join(
            f"{k}={v}"
            for k, v in {
                "function": self.function,
                "kwargs": self.kwargs,
                "queue": self.queue.name,
                "job_id": self.job_id,
                "process_ms": self.duration("process"),
                "start_ms": self.duration("start"),
                "total_ms": self.duration("total"),
                "attempts": self.attempts,
                "result": self.result,
                "error": self.error,
                "status": self.status,
            }.items()
            if v is not None
        )
        return f"Job<{kwargs}>"

    def duration(self, kind):
        if kind == "process":
            return self._duration(self.completed, self.started)
        if kind == "start":
            return self._duration(self.started, self.enqueued)
        if kind == "total":
            return self._duration(self.completed, self.enqueued)
        raise ValueError(f"Unknown duration type: {kind}")

    def _duration(self, a, b):
        return a - b if a and b else None

    @property
    def stuck(self):
        current = now()
        return (self.status == Status.ACTIVE) and (
            (self.started and seconds(current - self.started) > self.timeout)
            or (
                self.touched
                and self.heartbeat
                and seconds(current - self.touched) > self.heartbeat
            )
        )

    async def abort(self):
        await self.queue.abort(self)

    async def enqueue(self, queue=None):
        queue = queue or self.queue
        assert queue, "Queue unspecified"
        await queue.enqueue(self)

    async def finish(self, status, *, result=None, error=None):
        await self.queue.finish(self, status, result=result, error=error)

    async def retry(self, error):
        await self.queue.retry(self, error)

    async def update(self):
        await self.queue.update(self)
