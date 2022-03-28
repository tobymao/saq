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


TERMINAL_STATUSES = {Status.COMPLETE, Status.FAILED, Status.ABORTED}
UNSUCCESSFUL_TERMINAL_STATUSES = TERMINAL_STATUSES - {Status.COMPLETE}


class JobError(Exception):
    def __init__(self, job):
        super().__init__(
            f"Job {job.id} {job.status}\n\nThe above job failed with the following error:\n\n{job.error}"
        )
        self.job = job


@dataclasses.dataclass
class CronJob:
    """
    Allows scheduling of repeated jobs with cron syntax.

    function: the async function to run
    cron: cron string for a job to be repeated, uses croniter
    unique: unique jobs only one once per queue, defaults true

    Remaining kwargs are pass through to Job
    """

    function: typing.Callable
    cron: str
    unique: bool = True
    timeout: typing.Optional[int] = None
    heartbeat: typing.Optional[int] = None
    retries: typing.Optional[int] = None
    ttl: typing.Optional[int] = None


@dataclasses.dataclass
class Job:
    """
    Main job class representing a run of a function.

    User Provided Arguments
        function: the async function name to run
        kwargs: kwargs to pass to the function
        queue: the saq.Queue object associated with the job
        key: unique identifier of a job, defaults to uuid1, can be passed in to avoid duplicate jobs
        timeout: the maximum amount of time a job can run for in seconds, defaults to 10 (0 means disabled)
        heartbeat: the maximum amount of time a job can survive without a heartebat in seconds, defaults to 0 (disabled)
            a heartbeat can be triggered manually within a job by calling await job.update()
        retries: the maximum number of attempts to retry a job, defaults to 1
        ttl: the maximum time in seconds to store information about a job including results, defaults to 600
        scheduled: epoch seconds for when the job should be scheduled, defaults to 0 (schedule right away)
        progress: job progress 0.0..1.0
    Framework Set Properties
        attempts: number of attempts a job has had
        completed: job completion time epoch seconds
        queued: job enqueued time epoch seconds
        started: job started time epoch seconds
        touched: job touched/updated time epoch seconds
        results: dictionary containing the results, this is the return of the function provided, must be serializable, defaults to json
        error: stack trace if an runtime error occurs
        status: Status Enum, defaulst to Status.New
    """

    function: str
    kwargs: typing.Optional[dict] = None
    queue: typing.Optional["Queue"] = None
    key: str = dataclasses.field(default_factory=uuid1)
    timeout: int = 10
    heartbeat: int = 0
    retries: int = 1
    ttl: int = 600
    scheduled: int = 0
    progress: float = 0.0
    attempts: int = 0
    completed: int = 0
    queued: int = 0
    started: int = 0
    touched: int = 0
    result: typing.Any = None
    error: typing.Optional[str] = None
    status: Status = Status.NEW

    def __repr__(self):
        kwargs = ", ".join(
            f"{k}={v}"
            for k, v in {
                "function": self.function,
                "kwargs": self.kwargs,
                "queue": self.queue.name,
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
            }.items()
            if v is not None
        )
        return f"Job<{kwargs}>"

    def __hash__(self):
        return hash(self.key)

    @property
    def id(self):
        return f"saq:job:{self.key}"

    @property
    def abort_id(self):
        return f"saq:abort:{self.key}"

    def to_dict(self):
        return {
            k: v.name if k == "queue" else v
            for k, v in self.__dict__.items()
            if v != self.__dataclass_fields__[k].default  # pylint: disable=no-member
        }

    def duration(self, kind):
        """
        Returns the duration of the job given kind.

        Kind can be process (how long it took to process),
        start (how long it took to start), or total.
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

    def _duration(self, a, b):
        return a - b if a and b else None

    @property
    def stuck(self):
        """Checks if an active job is passed it's timeout or heartbeat."""
        current = now()
        return (self.status == Status.ACTIVE) and (
            seconds(current - self.started) > self.timeout
            or (self.heartbeat and seconds(current - self.touched) > self.heartbeat)
        )

    async def enqueue(self, queue=None):
        """
        Enqueues the job to it's queue or a provided one.

        A job that already has a queue cannot be re-enqueued. Job uniqueness is determined by its id.
        If a job has already been queued, it will update it's properties to match what is stored in the db.
        """
        queue = queue or self.queue
        assert queue, "Queue unspecified"
        if not await queue.enqueue(self):
            await self.refresh()

    async def abort(self, error, ttl=5):
        """Tries to abort the job."""
        await self.queue.abort(self, error, ttl=ttl)

    async def finish(self, status, *, result=None, error=None):
        """Finishes the job with a Job.Status, result, and or error."""
        await self.queue.finish(self, status, result=result, error=error)

    async def retry(self, error):
        """Retries the job by removing it from active and requeueing it."""
        await self.queue.retry(self, error)

    async def update(self, **kwargs):
        """
        Updates the stored job in redis.

        Set properties with passed in kwargs.
        """
        for k, v in kwargs.items():
            setattr(self, k, v)
        await self.queue.update(self)

    async def refresh(self, until_complete=None):
        """
        Refresh the current job with the latest data from the db.

        until_complete: None or Numeric seconds. if None (default), don't wait,
            else wait seconds until the job is complete or the interval has been reached. 0 means wait forever
        """
        job = await self.queue.job(self.id)

        if not job:
            raise RuntimeError(f"{self} doesn't exist")

        self.replace(job)

        if until_complete is not None and not self.completed:

            async def callback(_id, status):
                if status in TERMINAL_STATUSES:
                    await self.refresh()
                    return True

            await self.queue.listen(self, callback, until_complete)

    async def wait_for_result(self, timeout=None):
        """
        Wait for the current job to complete and return its result.

        If the job is unsuccessful, this raises a JobError.

        timeout: None or Numeric seconds. if None (default) or 0, wait forever.
        """
        await self.refresh(until_complete=timeout or 0)

        if self.status in UNSUCCESSFUL_TERMINAL_STATUSES:
            raise JobError(self)

        return self.result

    def replace(self, job):
        """Replace current attributes with job attributes."""
        for field in job.__dataclass_fields__:
            setattr(self, field, getattr(job, field))
