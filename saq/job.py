"""
Jobs
"""

from __future__ import annotations

import dataclasses
import enum
import typing as t

from saq.utils import exponential_backoff, now, seconds, uuid1


if t.TYPE_CHECKING:
    from saq.queue import Queue
    from saq.types import DurationKind, Function

ABORT_ID_PREFIX = "saq:abort:"


def get_default_job_key() -> str:
    return uuid1()


class Status(str, enum.Enum):
    """
    Queue Status
    """

    NEW = "new"
    DEFERRED = "deferred"
    QUEUED = "queued"
    ACTIVE = "active"
    ABORTED = "aborted"
    FAILED = "failed"
    COMPLETE = "complete"


TERMINAL_STATUSES = {Status.COMPLETE, Status.FAILED, Status.ABORTED}
UNSUCCESSFUL_TERMINAL_STATUSES = TERMINAL_STATUSES - {Status.COMPLETE}


@dataclasses.dataclass
class CronJob:
    """
    Allows scheduling of repeated jobs with cron syntax.

    Args:
        function (saq.types.Function): the async function to run
        cron (str): cron string for a job to be repeated, uses croniter
        unique (bool): unique jobs only one once per queue, defaults true

    **The remaining kwargs are pass through to Job:** (see equivalent field in Job for more)

    * timeout
    * heartbeat
    * retries
    * ttl
    """

    function: Function
    cron: str
    unique: bool = True
    timeout: int | None = None
    heartbeat: int | None = None
    retries: int | None = None
    ttl: int | None = None


@dataclasses.dataclass
class Job:
    """
    Main job class representing a run of a function.

    **User specified Arguments:**

    Args:
        function (str): the async function name to run
        kwargs (dict[str, Any] | None): kwargs to pass to the function
        queue (saq.queue.Queue): the saq.Queue object associated with the job
        key (str): unique identifier of a job, defaults to uuid1, can be passed in to avoid duplicate jobs
        timeout (int): the maximum amount of time a job can run for in seconds, defaults to 10 (0 means disabled)
        heartbeat (int): the maximum amount of time a job can survive without a heartbeat in seconds, defaults to 0 (disabled)
            a heartbeat can be triggered manually within a job by calling await job.update()
        retries (int): the maximum number of attempts to retry a job, defaults to 1
        ttl (int): the maximum time in seconds to store information about a job including results, defaults to 600
            (0 means indefinitely, -1 means disabled)
        retry_delay (float): seconds to delay before retrying the job
        retry_backoff (bool | float): If true, use exponential backoff for retry delays.
            The first retry will have whatever retry_delay is.
            The second retry will have retry_delay*2. The third retry will have retry_delay*4. And so on.
            This always includes jitter, where the final retry delay is a random number between 0 and the calculated retry delay.
            If retry_backoff is set to a number, that number is the maximum retry delay, in seconds.
        scheduled (int): epoch seconds for when the job should be scheduled, defaults to 0 (schedule right away)
        progress (float): job progress 0.0..1.0
        meta (dict): arbitrary metadata to attach to the job

    **Framework Set Properties:**
    Don't set these, but you can read them.

    Parameters:
        attempts (int): number of attempts a job has had
        completed (int): job completion time epoch seconds
        queued (int): job enqueued time epoch seconds
        started (int): job started time epoch seconds
        touched (int): job touched/updated time epoch seconds
        result: payload containing the results, this is the return of the function provided, must be serializable, defaults to json
        error (str | None): stack trace if a runtime error occurs
        status (Status): Status Enum, default to Status.New
    """

    function: str
    kwargs: dict[str, t.Any] | None = None
    queue: Queue | None = None
    key: str = dataclasses.field(default_factory=get_default_job_key)
    timeout: int = 10
    heartbeat: int = 0
    retries: int = 1
    ttl: int = 600
    retry_delay: float = 0.0
    retry_backoff: bool | float = False
    scheduled: int = 0
    progress: float = 0.0
    attempts: int = 0
    completed: int = 0
    queued: int = 0
    started: int = 0
    touched: int = 0
    result: t.Any = None
    error: str | None = None
    status: Status = Status.NEW
    meta: dict[t.Any, t.Any] = dataclasses.field(default_factory=dict)

    _EXCLUDE_NON_FULL = {
        "kwargs",
        "scheduled",
        "progress",
        "total_ms",
        "result",
        "error",
        "status",
        "meta",
    }

    def info(self, full: bool = False) -> str:
        """
        String with Job info

        Args:
            full: If true, will list the full kwargs for the Job, else an abridged version.
        """
        # Using an exclusion list preserves order for kwargs below
        excluded = set() if full else self._EXCLUDE_NON_FULL
        kwargs = ", ".join(
            f"{k}={v}"
            for k, v in {
                "function": self.function,
                "kwargs": self.kwargs,
                "queue": self.get_queue().name,
                "id": self.id,
                "scheduled": self.scheduled,
                "progress": self.progress,
                "process_ms": self.duration("process"),
                "start_ms": self.duration("start"),
                "total_ms": self.duration("total"),
                "attempts": self.attempts,
                "result": self.result,
                "error": self.error,
                "status": self.status,
                "meta": self.meta,
            }.items()
            if v is not None and k not in excluded
        )
        return f"Job<{kwargs}>"

    def __repr__(self) -> str:
        return self.info(True)

    def __hash__(self) -> int:
        return hash(self.key)

    @property
    def id(self) -> str:
        """Full Job ID"""
        return self.get_queue().job_id(self.key)

    @property
    def abort_id(self) -> str:
        return f"{ABORT_ID_PREFIX}{self.key}"

    def to_dict(self) -> dict[str, t.Any]:
        """
        Serialises the Job to dict
        """
        result = {}
        for field in dataclasses.fields(self):
            key = field.name
            value = getattr(self, key)
            if value == field.default:
                continue
            if key == "meta" and not value:
                continue
            if key == "queue" and value:
                value = value.name
            result[key] = value
        return result

    def duration(self, kind: DurationKind) -> int | None:
        """
        Returns the duration of the job given kind.

        Args:
            Kind (DurationKind): The kind of duration type, can be:
                * `process` (how long it took to process)
                * `start` (how long it took to start)
                * `total`
                * `running`
        """
        if kind == "process":
            return self._duration(self.completed, self.started)
        if kind == "start":
            return self._duration(self.started, self.queued)
        if kind == "total":
            return self._duration(self.completed, self.queued)
        if kind == "running":
            return self._duration(now(), self.started)
        raise ValueError(f"Unknown duration type: {kind}")

    def _duration(self, a: int, b: int) -> int | None:
        return a - b if a and b else None

    @property
    def stuck(self) -> bool:
        """Checks if an active job is passed its timeout or heartbeat."""
        current = now()
        return (self.status == Status.ACTIVE) and bool(
            (self.timeout and seconds(current - self.started) > self.timeout)
            or (self.heartbeat and seconds(current - self.touched) > self.heartbeat)
        )

    @property
    def retryable(self) -> bool:
        return self.retries > self.attempts

    def next_retry_delay(self) -> float:
        if self.retry_backoff is not False:
            max_delay = None if self.retry_backoff is True else self.retry_backoff
            return exponential_backoff(
                attempts=self.attempts,
                base_delay=self.retry_delay,
                max_delay=max_delay,
                jitter=True,
            )
        return self.retry_delay

    async def enqueue(self, queue: Queue | None = None) -> None:
        """
        Enqueues the job to it's queue or a provided one.

        A job that already has a queue cannot be re-enqueued. Job uniqueness is determined by its id.
        If a job has already been queued, it will update its properties to match what is stored in the db.
        """
        queue = queue or self.get_queue()
        if not await queue.enqueue(self):
            await self.refresh()

    async def abort(self, error: str, ttl: int = 5) -> None:
        """Tries to abort the job."""
        await self.get_queue().abort(self, error, ttl=ttl)

    async def finish(
        self, status: Status, *, result: t.Any = None, error: str | None = None
    ) -> None:
        """Finishes the job with a Job.Status, result, and or error."""
        await self.get_queue().finish(self, status, result=result, error=error)

    async def retry(self, error: str | None) -> None:
        """Retries the job by removing it from active and enqueueing it again."""
        await self.get_queue().retry(self, error)

    async def update(self, **kwargs: t.Any) -> None:
        """
        Updates the stored job in redis.

        Set properties with passed in kwargs.
        """
        for k, v in kwargs.items():
            setattr(self, k, v)
        await self.get_queue().update(self)

    async def refresh(self, until_complete: float | None = None) -> None:
        """
        Refresh the current job with the latest data from the db.

        Args:
            until_complete (float | None): None or Numeric seconds. if None (default), don't wait,
                else wait seconds until the job is complete or the interval has been reached. 0 means wait forever
        """
        job = await self.get_queue().job(self.key)

        if not job:
            raise RuntimeError(f"{self} doesn't exist")

        self.replace(job)

        if until_complete is not None and not self.completed:

            async def callback(_id: str, status: Status) -> bool:
                return status in TERMINAL_STATUSES

            await self.get_queue().listen([self.key], callback, until_complete)
            await self.refresh()

    def replace(self, job: Job) -> None:
        """Replace current attributes with job attributes."""
        for field in dataclasses.fields(job):
            setattr(self, field.name, getattr(job, field.name))

    def get_queue(self) -> Queue:
        if self.queue is None:
            raise TypeError(
                "`Job` must be associated with a `Queue` before this operation can proceed"
            )
        return self.queue
