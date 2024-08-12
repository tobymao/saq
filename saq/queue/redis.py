"""
Redis Queue
"""

from __future__ import annotations

import asyncio
import logging
import time
import typing as t
from collections import defaultdict

from saq.errors import MissingDependencyError
from saq.job import (
    Job,
    Status,
)
from saq.queue.base import Queue, logger
from saq.utils import millis, now, seconds, uuid1

try:
    from redis import asyncio as aioredis
except ModuleNotFoundError as e:
    raise MissingDependencyError(
        "Missing dependencies for Redis. Install them with `pip install saq[redis]`."
    ) from e

if t.TYPE_CHECKING:
    from collections.abc import Iterable

    from redis.asyncio.client import Redis, PubSub
    from redis.commands.core import AsyncScript

    from saq.types import (
        CountKind,
        DumpType,
        ListenCallback,
        LoadType,
        QueueInfo,
        QueueStats,
        VersionTuple,
    )

    # PubSubMultiplexer Queue
    Q = asyncio.Queue[dict]

ID_PREFIX = "saq:job:"


class RedisQueue(Queue):
    """
    Queue is used to interact with redis.

    Args:
        redis: instance of redis.asyncio pool
        name: name of the queue (default "default")
        dump: lambda that takes a dictionary and outputs bytes (default `json.dumps`)
        load: lambda that takes str or bytes and outputs a python dictionary (default `json.loads`)
        max_concurrent_ops: maximum concurrent operations. (default 20)
            This throttles calls to `enqueue`, `job`, and `abort` to prevent the Queue
            from consuming too many Redis connections.
    """

    @classmethod
    def from_url(cls: type[RedisQueue], url: str, **kwargs: t.Any) -> RedisQueue:
        """Create a queue with a redis url a name."""
        return cls(aioredis.from_url(url), **kwargs)

    def __init__(  # pylint: disable=too-many-arguments
        self,
        redis: Redis[bytes],
        name: str = "default",
        dump: DumpType | None = None,
        load: LoadType | None = None,
        max_concurrent_ops: int = 20,
    ) -> None:
        super().__init__(name=name, dump=dump, load=load)

        self.redis = redis
        self.uuid: str = uuid1()
        self._version: VersionTuple | None = None
        self._schedule_script: AsyncScript | None = None
        self._enqueue_script: AsyncScript | None = None
        self._cleanup_script: AsyncScript | None = None
        self._incomplete = self.namespace("incomplete")
        self._queued = self.namespace("queued")
        self._active = self.namespace("active")
        self._schedule = self.namespace("schedule")
        self._sweep = self.namespace("sweep")
        self._stats = self.namespace("stats")
        self._op_sem = asyncio.Semaphore(max_concurrent_ops)
        self._pubsub = PubSubMultiplexer(
            redis.pubsub(), prefix=f"{ID_PREFIX}{self.name}"
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}<redis={self.redis}, name='{self.name}'>"

    def job_id(self, job_key: str) -> str:
        return f"{ID_PREFIX}{self.name}:{job_key}"

    def job_key_from_id(self, job_id: str) -> str:
        return job_id[len(f"{ID_PREFIX}{self.name}:") :]

    def namespace(self, key: str) -> str:
        return ":".join(["saq", self.name, key])

    def serialize(self, job: Job) -> str:
        return self._dump(job.to_dict())

    def deserialize(self, job_bytes: bytes | None) -> Job | None:
        if not job_bytes:
            return None

        job_dict = self._load(job_bytes)
        if job_dict.pop("queue") != self.name:
            raise ValueError(f"Job {job_dict} fetched by wrong queue: {self.name}")
        return Job(**job_dict, queue=self)

    async def disconnect(self) -> None:
        await self._pubsub.close()
        if hasattr(self.redis, "aclose"):
            await self.redis.aclose()
        else:
            await self.redis.close()
        await self.redis.connection_pool.disconnect()

    async def version(self) -> VersionTuple:
        if self._version is None:
            info = await self.redis.info()
            self._version = tuple(int(i) for i in str(info["redis_version"]).split("."))
        return self._version

    async def info(
        self, jobs: bool = False, offset: int = 0, limit: int = 10
    ) -> QueueInfo:
        """
        Returns info on the queue

        Args:
            jobs: Include job info (default False)
            offset: Offset of job info for pagination (default 0)
            limit: Max length of job info (default 10)
        """
        # pylint: disable=too-many-locals
        worker_uuids = []

        for key in await self.redis.zrangebyscore(self._stats, now(), "inf"):
            key_str = key.decode("utf-8")
            *_, worker_uuid = key_str.split(":")
            worker_uuids.append(worker_uuid)

        worker_stats = await self.redis.mget(
            self.namespace(f"stats:{worker_uuid}") for worker_uuid in worker_uuids
        )

        worker_info = {}
        for worker_uuid, stats in zip(worker_uuids, worker_stats):
            if stats:
                stats_obj = self._load(stats)
                worker_info[worker_uuid] = stats_obj

        queued = await self.count("queued")
        active = await self.count("active")
        incomplete = await self.count("incomplete")

        if jobs:
            deserialized_jobs = (
                self.deserialize(job_bytes)
                for job_bytes in await self.redis.mget(
                    (await self.redis.lrange(self._active, offset, limit - 1))
                    + (
                        await self.redis.zrange(
                            self._incomplete, offset, limit - 1, withscores=False
                        )
                    )
                )
            )
            job_info = list(
                {
                    job["key"]: job
                    for job in (
                        job.to_dict() for job in deserialized_jobs if job is not None
                    )
                }.values()
            )
        else:
            job_info = []

        return {
            "workers": worker_info,
            "name": self.name,
            "queued": queued,
            "active": active,
            "scheduled": incomplete - queued - active,
            "jobs": job_info,
        }

    async def stats(self, ttl: int = 60) -> QueueStats:
        """
        Returns & updates stats on the queue

        Args:
            ttl: Time-to-live of stats saved in Redis
        """
        current = now()
        stats: QueueStats = {
            "complete": self.complete,
            "failed": self.failed,
            "retried": self.retried,
            "aborted": self.aborted,
            "uptime": current - self.started,
        }
        async with self.redis.pipeline(transaction=True) as pipe:
            key = self.namespace(f"stats:{self.uuid}")
            await (
                pipe.setex(key, ttl, self._dump(stats))
                .zremrangebyscore(self._stats, 0, current)
                .zadd(self._stats, {key: current + millis(ttl)})
                .expire(self._stats, ttl)
                .execute()
            )
        return stats

    async def count(self, kind: CountKind) -> int:
        """
        Gets count of the kind provided by CountKind

        Args:
            kind: The type of Kind you want counts info on
        """
        if kind == "queued":
            return await self.redis.llen(self._queued)
        if kind == "active":
            return await self.redis.llen(self._active)
        if kind == "incomplete":
            return await self.redis.zcard(self._incomplete)
        raise ValueError("Can't count unknown type {kind}")

    async def schedule(self, lock: int = 1) -> t.Any:
        if not self._schedule_script:
            self._schedule_script = self.redis.register_script(
                """
                if redis.call('EXISTS', KEYS[1]) == 0 then
                    redis.call('SETEX', KEYS[1], ARGV[1], 1)
                    local jobs = redis.call('ZRANGEBYSCORE', KEYS[2], 1, ARGV[2])

                    if next(jobs) then
                        for _, v in ipairs(jobs) do
                            redis.call('ZADD', KEYS[2], 0, v)
                            redis.call('RPUSH', KEYS[3], v)
                        end
                    end

                    return jobs
                end
                """
            )

        return await self._schedule_script(
            keys=[self._schedule, self._incomplete, self._queued],
            args=[lock, seconds(now())],
        )

    async def sweep(self, lock: int = 60, abort: float = 5.0) -> list[t.Any]:
        if not self._cleanup_script:
            self._cleanup_script = self.redis.register_script(
                """
                if redis.call('EXISTS', KEYS[1]) == 0 then
                    redis.call('SETEX', KEYS[1], ARGV[1], 1)
                    return redis.call('LRANGE', KEYS[2], 0, -1)
                end
                """
            )

        job_ids = await self._cleanup_script(
            keys=[self._sweep, self._active], args=[lock], client=self.redis
        )

        swept = []
        if job_ids:
            for job_id, job_bytes in zip(job_ids, await self.redis.mget(job_ids)):
                job = self.deserialize(job_bytes)

                if job:
                    # in rare cases a sweep may occur between a dequeue and actual processing.
                    # in that case, the job status is still set to queued, but only because it hasn't
                    # had its status updated yet. try refreshing after a short sleep to pick up the latest status.
                    if job.status == Status.QUEUED:
                        await asyncio.sleep(0.1)
                        await job.refresh()
                        stuck = job.status == Status.QUEUED
                    else:
                        stuck = job.status != Status.ACTIVE or job.stuck

                    if stuck:
                        swept.append(job_id)
                        await self.abort(job, error="swept")

                        logger.info(
                            "Sweeping job %s",
                            job.info(logger.isEnabledFor(logging.DEBUG)),
                        )

                        if job.retryable:
                            try:
                                await job.refresh(abort)
                            except asyncio.TimeoutError:
                                logger.info("Could not abort job %s", job_id)
                            finally:
                                await self.retry(job, error="swept")
                        else:
                            await job.finish(Status.ABORTED, error="swept")
                else:
                    swept.append(job_id)

                    async with self.redis.pipeline(transaction=True) as pipe:
                        await (
                            pipe.lrem(self._active, 0, job_id)
                            .zrem(self._incomplete, job_id)
                            .execute()
                        )
                    logger.info("Sweeping missing job %s", job_id)

        return swept

    async def notify(self, job: Job) -> None:
        await self.redis.publish(job.id, job.status)

    async def update(self, job: Job) -> None:
        job.touched = now()
        await self.redis.set(job.id, self.serialize(job))
        await self.notify(job)

    async def job(self, job_key: str) -> Job | None:
        job_id = self.job_id(job_key)
        return await self._get_job_by_id(job_id)

    async def _get_job_by_id(self, job_id: bytes | str) -> Job | None:
        async with self._op_sem:
            return self.deserialize(await self.redis.get(job_id))

    async def abort(self, job: Job, error: str, ttl: float = 5) -> None:
        async with self._op_sem:
            async with self.redis.pipeline(transaction=True) as pipe:
                dequeued, *_ = await (
                    pipe.lrem(self._queued, 0, job.id)
                    .zrem(self._incomplete, job.id)
                    .expire(job.id, ttl + 1)
                    .setex(job.abort_id, ttl, error)
                    .execute()
                )

            if dequeued:
                await job.finish(Status.ABORTED, error=error)
                await self.redis.delete(job.abort_id)
            else:
                await self.redis.lrem(self._active, 0, job.id)

    async def retry(self, job: Job, error: str | None) -> None:
        self._update_job_for_retry(job, error)
        job_id = job.id
        next_retry_delay = job.next_retry_delay()

        async with self.redis.pipeline(transaction=True) as pipe:
            pipe = pipe.lrem(self._active, 1, job_id)
            pipe = pipe.lrem(self._queued, 1, job_id)
            if next_retry_delay:
                scheduled = time.time() + next_retry_delay
                pipe = pipe.zadd(self._incomplete, {job_id: scheduled})
            else:
                pipe = pipe.zadd(self._incomplete, {job_id: job.scheduled})
                pipe = pipe.rpush(self._queued, job_id)
            await pipe.set(job_id, self.serialize(job)).execute()
            self.retried += 1
            await self.notify(job)
            logger.info("Retrying %s", job.info(logger.isEnabledFor(logging.DEBUG)))

    async def finish(
        self,
        job: Job,
        status: Status,
        *,
        result: t.Any = None,
        error: str | None = None,
    ) -> None:
        self._update_job_for_finish(job, status, result=result, error=error)
        job_id = job.id

        async with self.redis.pipeline(transaction=True) as pipe:
            pipe = pipe.lrem(self._active, 1, job_id).zrem(self._incomplete, job_id)

            if job.ttl > 0:
                pipe = pipe.setex(job_id, job.ttl, self.serialize(job))
            elif job.ttl == 0:
                pipe = pipe.set(job_id, self.serialize(job))
            else:
                pipe.delete(job_id)

            await pipe.execute()

            if status == Status.COMPLETE:
                self.complete += 1
            elif status == Status.FAILED:
                self.failed += 1
            elif status == Status.ABORTED:
                self.aborted += 1

            await self.notify(job)
            logger.info("Finished %s", job.info(logger.isEnabledFor(logging.DEBUG)))

    async def dequeue(self, timeout: float = 0) -> Job | None:
        if await self.version() < (6, 2, 0):
            job_id = await self.redis.brpoplpush(
                self._queued, self._active, timeout  # type:ignore[arg-type]
            )
        else:
            job_id = await self.redis.blmove(
                self._queued,
                self._active,
                timeout,
                "LEFT",
                "RIGHT",
            )
        if job_id is not None:
            return await self._get_job_by_id(job_id)

        logger.debug("Dequeue timed out")
        return None

    async def enqueue(self, job_or_func: str | Job, **kwargs: t.Any) -> Job | None:
        """
        Enqueue a job by instance or string.

        Example:
            .. code-block::

                job = await queue.enqueue("add", a=1, b=2)
                print(job.id)

        Args:
            job_or_func: The job or function to enqueue.
                If a job instance is passed in, it's properties are overriden.
            kwargs: Kwargs can be arguments of the function or properties of the job.

        Returns:
            If the job has already been enqueued, this returns None, else Job
        """
        job = self._create_job_for_enqueue(job_or_func, **kwargs)

        if not self._enqueue_script:
            self._enqueue_script = self.redis.register_script(
                """
                if not redis.call('ZSCORE', KEYS[1], KEYS[2]) and redis.call('EXISTS', KEYS[4]) == 0 then
                    redis.call('SET', KEYS[2], ARGV[1])
                    redis.call('ZADD', KEYS[1], ARGV[2], KEYS[2])
                    if ARGV[2] == '0' then redis.call('RPUSH', KEYS[3], KEYS[2]) end
                    return 1
                else
                    return nil
                end
                """
            )

        await self._before_enqueue(job)

        async with self._op_sem:
            if not await self._enqueue_script(
                keys=[self._incomplete, job.id, self._queued, job.abort_id],
                args=[self.serialize(job), job.scheduled],
                client=self.redis,
            ):
                return None

        logger.info("Enqueuing %s", job.info(logger.isEnabledFor(logging.DEBUG)))
        return job

    async def listen(
        self,
        job_keys: Iterable[str],
        callback: ListenCallback,
        timeout: float | None = 10,
    ) -> None:
        """
        Listen to updates on jobs.

        Args:
            job_keys: sequence of job keys
            callback: callback function, if it returns truthy, break
            timeout: if timeout is truthy, wait for timeout seconds
        """
        job_ids = [self.job_id(job_key) for job_key in job_keys]
        if not job_ids:
            return

        queue = await self._pubsub.subscribe(*job_ids)

        async def listen() -> None:
            while True:
                message = await queue.get()
                queue.task_done()
                job_id = message["channel"].decode("utf-8")
                job_key = self.job_key_from_id(job_id)
                status = Status[message["data"].decode("utf-8").upper()]
                if asyncio.iscoroutinefunction(callback):
                    stop = await callback(job_key, status)
                else:
                    stop = callback(job_key, status)

                if stop:
                    break

        try:
            if timeout:
                await asyncio.wait_for(listen(), timeout)
            else:
                await listen()
        finally:
            await self._pubsub.unsubscribe(queue)

    async def get_many(self, keys: Iterable[str]) -> list[bytes | None]:
        return await self.redis.mget(keys)

    async def delete(self, key: str) -> None:
        await self.redis.delete(key)


class PubSubMultiplexer:
    """
    Handle multiple in-process channels over a single Redis channel.

    We use pubsub for realtime job updates. Each pubsub instance needs
    a connection, and that connection can't be reused when its done (that's
    an assumption, based on redis-py not releasing the connection and
    this comment: https://github.com/redis/go-redis/issues/785#issuecomment-394596158).
    So we use a single pubsub instance that listens to all job changes on
    the queue and handle message routing in-process.
    """

    def __init__(self, pubsub: PubSub, prefix: str):
        self.prefix = prefix
        self.pubsub = pubsub
        self._subscriptions: t.Dict[bytes, t.Set[Q]] = defaultdict(set)
        self._queues: t.Dict[Q, t.Set[bytes]] = {}
        self._daemon_task: t.Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    async def _daemon(self) -> None:
        while True:
            try:
                message = await self.pubsub.get_message(timeout=None)  # type: ignore
                if message and message["type"] == "pmessage":
                    for queue in self._subscriptions[message["channel"]]:
                        queue.put_nowait(message)
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Failed to consume message")

    async def start(self) -> None:
        if not self._daemon_task:
            async with self._lock:
                if not self._daemon_task:
                    await self.pubsub.psubscribe(f"{self.prefix}*")
                    self._daemon_task = asyncio.create_task(self._daemon())

    async def close(self) -> None:
        await self.pubsub.punsubscribe()
        if self._daemon_task:
            self._daemon_task.cancel()
            await self._daemon_task
            self._daemon_task = None
            self._subscriptions.clear()
            self._queues.clear()

    async def subscribe(self, *channels: str) -> Q:
        await self.start()
        queue: Q = asyncio.Queue()

        # Use bytes so the daemon needs to do less work
        bchannels = [c.encode() for c in channels]

        self._queues[queue] = set(bchannels)
        for channel in bchannels:
            self._subscriptions[channel].add(queue)
        return queue

    async def unsubscribe(self, queue: Q) -> None:
        for channel in self._queues.pop(queue, []):
            self._subscriptions[channel].remove(queue)
