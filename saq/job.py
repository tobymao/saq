import asyncio
import dataclasses
import enum
from functools import wraps
import logging
import typing

from saq.utils import now, seconds, uuid1, exponential_backoff

if typing.TYPE_CHECKING:
    from saq.queue import Queue

logger = logging.getLogger("saq")

ABORT_ID_PREFIX = "saq:abort:"


def get_default_job_key():
    return uuid1()


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
        ttl: the maximum time in seconds to store information about a job including results, defaults to 600 (0 means indefinitely, -1 means disabled)
        retry_delay: seconds to delay before retrying the job
        retry_backoff: If true, use exponential backoff for retry delays.
            The first retry will have whatever retry_delay is.
            The second retry will have retry_delay*2. The third retry will have retry_delay*4. And so on.
            This always includes jitter, where the final retry delay is a random number between 0 and the calculated retry delay.
            If retry_backoff is set to a number, that number is the maximum retry delay, in seconds.
        scheduled: epoch seconds for when the job should be scheduled, defaults to 0 (schedule right away)
        progress: job progress 0.0..1.0
        meta: arbitrary metadata to attach to the job
    Framework Set Properties
        attempts: number of attempts a job has had
        completed: job completion time epoch seconds
        queued: job enqueued time epoch seconds
        started: job started time epoch seconds
        touched: job touched/updated time epoch seconds
        results: payload containing the results, this is the return of the function provided, must be serializable, defaults to json
        error: stack trace if an runtime error occurs
        status: Status Enum, defaulst to Status.New
    """

    function: str
    kwargs: typing.Optional[dict] = None
    queue: typing.Optional["Queue"] = None
    key: str = dataclasses.field(default_factory=get_default_job_key)
    timeout: int = 10
    heartbeat: int = 0
    retries: int = 1
    ttl: int = 600
    retry_delay: float = 0.0
    retry_backoff: typing.Union[bool, float] = False
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
    meta: dict = dataclasses.field(default_factory=dict)

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
                "meta": self.meta,
            }.items()
            if v is not None
        )
        return f"Job<{kwargs}>"

    def __hash__(self):
        return hash(self.key)

    @property
    def id(self):
        return self.queue.job_id(self.key)

    @classmethod
    def key_from_id(cls, job_id):
        return job_id.split(":")[-1]

    @property
    def abort_id(self):
        return f"{ABORT_ID_PREFIX}{self.key}"

    def to_dict(self):
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
        # if timeout == None, the if statement will fail for
        # TypeError: '>' not supported between instances of 'float' and 'NoneType'
        return (self.status == Status.ACTIVE) and (
            (self.timeout and seconds(current - self.started) > self.timeout)
            or (self.heartbeat and seconds(current - self.touched) > self.heartbeat)
        )

    def next_retry_delay(self):
        if self.retry_backoff:
            max_delay = self.retry_delay
            if max_delay is True:
                max_delay = None
            return exponential_backoff(
                attempts=self.attempts,
                base_delay=self.retry_delay,
                max_delay=max_delay,
                jitter=True,
            )
        return self.retry_delay

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
        job = await self.queue.job(self.key)

        if not job:
            raise RuntimeError(f"{self} doesn't exist")

        self.replace(job)

        if until_complete is not None and not self.completed:

            async def callback(_id, status):
                if status in TERMINAL_STATUSES:
                    return True

            await self.queue.listen([self.key], callback, until_complete)
            await self.refresh()

    def replace(self, job):
        """Replace current attributes with job attributes."""
        for field in job.__dataclass_fields__:
            setattr(self, field, getattr(job, field))


class PeriodicHeartbeat(object):
    """Class to manage agent Heartbeats while Job is running"""

    def __init__(self, job: Job) -> None:
        self._running: bool = True
        self._heartbeat_task: typing.Optional[asyncio.Task] = None
        self.job = job
        self.heartbeat_enabled: bool = job.heartbeat and job.heartbeat > 0
        self.time_between_ticks = (
            max(round(job.heartbeat / 2), 1) if self.heartbeat_enabled > 0 else 0
        )

    async def start(self, func: typing.Awaitable):
        """start

        Executes function with a periodic heartbeat.
        Args:
            func (typing.Awaitable): _description_

        Returns:
            _type_: _description_
        """
        if self.heartbeat_enabled:
            logger.info(
                f"starting heartbeat service for {self.job.id}. Ticking every {self.time_between_ticks} second(s)",
            )
        self._heartbeat_task = asyncio.create_task(self._periodically_publish())
        executed_func = await func
        try:
            self._heartbeat_task.cancel()
            await self._heartbeat_task
        except asyncio.CancelledError:
            pass
        finally:
            return executed_func

    async def stop(self):
        """Stop heartbeat service."""
        if self.heartbeat_enabled:
            logger.info(f"stopping heartbeat service for {self.job.id}")
        if self._running:
            self._running = False

    async def _periodically_publish(self):
        """Periodically publish heartbeat"""
        while self._running and self.heartbeat_enabled:
            logger.info(
                f"ticking heartbeat for {self.job.id}, and sleeping for {self.time_between_ticks} second(s)",
            )
            await self.job.update()
            await asyncio.sleep(self.time_between_ticks)


# decorator to perform a heartbeat in the background while a function is running
def monitored_job(func):
    @wraps(func)
    async def with_heartbeat(*args, **kwargs) -> typing.Dict[str, typing.Any]:
        context: typing.Dict[str, typing.Any] = args[0]
        job: Job = context["job"]
        heartbeat = PeriodicHeartbeat(job)
        try:
            result = await heartbeat.start(func(*args, **kwargs))
        finally:
            await heartbeat.stop()
            return result

    return with_heartbeat
