"""
Redis Queue
"""

from __future__ import annotations

import asyncio
import logging
import json
import time
import typing as t

from saq.errors import MissingDependencyError
from saq.job import (
    Job,
    Status,
)
from saq.multiplexer import Multiplexer
from saq.queue.base import Queue, logger
from saq.utils import millis, now, now_seconds

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
        VersionTuple,
        WorkerInfo,
    )

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

    def __init__(
        self,
        redis: Redis[bytes],
        name: str = "default",
        dump: DumpType | None = None,
        load: LoadType | None = None,
        max_concurrent_ops: int = 20,
    ) -> None:
        super().__init__(name=name, dump=dump, load=load)

        self.redis = redis
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
        self._pubsub = PubSubMultiplexer(redis.pubsub(), prefix=f"{ID_PREFIX}{self.name}")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}<redis={self.redis}, name='{self.name}'>"

    def job_id(self, job_key: str) -> str:
        return f"{ID_PREFIX}{self.name}:{job_key}"

    def job_key_from_id(self, job_id: str) -> str:
        return job_id[len(f"{ID_PREFIX}{self.name}:") :]

    def namespace(self, key: str) -> str:
        return ":".join(["saq", self.name, key])

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

    async def info(self, jobs: bool = False, offset: int = 0, limit: int = 10) -> QueueInfo:
        """
        Returns info on the queue

        Args:
            jobs: Include job info (default False)
            offset: Offset of job info for pagination (default 0)
            limit: Max length of job info (default 10)
        """
        worker_uuids = []

        for key in await self.redis.zrangebyscore(self._stats, now(), "inf"):
            key_str = key.decode("utf-8")
            *_, worker_uuid = key_str.split(":")
            worker_uuids.append(worker_uuid)

        worker_metadata = await self.redis.mget(
            self.namespace(f"worker_info:{worker_uuid}") for worker_uuid in worker_uuids
        )
        workers: dict[str, WorkerInfo] = {}
        worker_metadata_dict = dict(zip(worker_uuids, worker_metadata))
        for worker in worker_uuids:
            metadata = worker_metadata_dict.get(worker)
            if metadata:
                workers[worker] = json.loads(metadata)

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
                    for job in (job.to_dict() for job in deserialized_jobs if job is not None)
                }.values()
            )
        else:
            job_info = []

        return {
            "workers": workers,
            "name": self.name,
            "queued": queued,
            "active": active,
            "scheduled": incomplete - queued - active,
            "jobs": job_info,
        }

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

    async def schedule(self, lock: int = 1) -> t.List[str]:
        if not self._schedule_script:
            self._schedule_script = self.redis.register_script(
                """
                if redis.call('EXISTS', KEYS[1]) == 0 then
                    redis.call('SETEX', KEYS[1], ARGV[1], 1)
                    local jobs = redis.call('ZRANGEBYSCORE', KEYS[2], 1, ARGV[2])

                    for _, v in ipairs(jobs) do
                        redis.call('ZADD', KEYS[2], 0, v)
                        redis.call('RPUSH', KEYS[3], v)
                    end

                    return jobs
                end
                """
            )

        return [
            job_id.decode("utf-8")
            for job_id in await self._schedule_script(
                keys=[self._schedule, self._incomplete, self._queued],
                args=[lock, now_seconds()],
            )
            or []
        ]

    async def sweep(self, lock: int = 60, abort: float = 5.0) -> list[str]:
        if not self._cleanup_script:
            self._cleanup_script = self.redis.register_script(
                """
                local id_jobs = {}
                if redis.call('EXISTS', KEYS[1]) == 0 then
                    redis.call('SETEX', KEYS[1], ARGV[1], 1)
                    for i, v in ipairs(redis.call('LRANGE', KEYS[2], 0, -1)) do
                        id_jobs[i] = {v, redis.call('GET', v)}
                    end
                end
                return id_jobs
                """
            )

        id_jobs = await self._cleanup_script(
            keys=[self._sweep, self._active], args=[lock], client=self.redis
        )

        swept = []

        for job_id, job_bytes in id_jobs:
            job = self.deserialize(job_bytes)

            if job:
                if job.status != Status.ACTIVE or job.stuck:
                    logger.info(
                        "Sweeping job %s",
                        job.info(logger.isEnabledFor(logging.DEBUG)),
                    )
                    swept.append(job_id)

                    await self.abort(job, error="swept")

                    try:
                        await job.refresh(abort)
                    except asyncio.TimeoutError:
                        logger.info("Could not abort job %s", job_id)

                    if job.retryable:
                        await self.retry(job, error="swept")
                    else:
                        await self.finish(job, Status.ABORTED, error="swept")
            else:
                swept.append(job_id)

                async with self.redis.pipeline(transaction=True) as pipe:
                    await (
                        pipe.lrem(self._active, 0, job_id).zrem(self._incomplete, job_id).execute()
                    )
                logger.info("Sweeping missing job %s", job_id)

        return [job_id.decode("utf-8") for job_id in swept]

    async def notify(self, job: Job) -> None:
        await self.redis.publish(job.id, job.status)

    async def _update(self, job: Job, status: Status | None = None, **kwargs: t.Any) -> None:
        if not status:
            stored = await self.job(job.key)
            status = stored.status if stored else None
        job.status = status or job.status
        await self.redis.set(job.id, self.serialize(job))
        await self.notify(job)

    async def job(self, job_key: str) -> Job | None:
        job_id = self.job_id(job_key)
        return await self._get_job_by_id(job_id)

    async def jobs(self, job_keys: Iterable[str]) -> t.List[Job | None]:
        return [
            self.deserialize(job_bytes)
            for job_bytes in await self.redis.mget(self.job_id(key) for key in job_keys)
        ]

    async def iter_jobs(
        self,
        statuses: t.List[Status] = list(Status),
        batch_size: int = 100,
    ) -> t.AsyncIterator[Job]:
        cursor = 0
        while True:
            cursor, job_ids = await self.redis.scan(
                cursor=cursor, match=self.job_id("*"), count=batch_size
            )
            statuses_set = set(statuses)

            for job in await self.jobs(
                self.job_key_from_id(job_id.decode("utf-8")) for job_id in job_ids
            ):
                if job and job.status in statuses_set:
                    yield job

            if cursor <= 0:
                break

    async def _get_job_by_id(self, job_id: bytes | str) -> Job | None:
        async with self._op_sem:
            return self.deserialize(await self.redis.get(job_id))

    async def abort(self, job: Job, error: str, ttl: float = 5) -> None:
        async with self._op_sem:
            async with self.redis.pipeline(transaction=True) as pipe:
                job.status = Status.ABORTING
                job.error = error

                dequeued, *_ = await (
                    pipe.lrem(self._queued, 0, job.id)
                    .zrem(self._incomplete, job.id)
                    .set(job.id, self.serialize(job))
                    .setex(job.abort_id, ttl, error)
                    .publish(job.id, job.status)
                    .execute()
                )

            if dequeued:
                await self.finish(job, Status.ABORTED, error=error)
                await self.redis.delete(job.abort_id)
            else:
                await self.redis.lrem(self._active, 0, job.id)

    async def dequeue(self, timeout: float = 0) -> Job | None:
        if await self.version() < (6, 2, 0):
            job_id = await self.redis.brpoplpush(
                self._queued,
                self._active,
                timeout,  # type:ignore[arg-type]
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

        async for message in self._pubsub.listen(*job_ids, timeout=timeout):
            job_id = message["channel"]
            job_key = self.job_key_from_id(job_id)
            status = Status[message["data"].decode("utf-8").upper()]
            if asyncio.iscoroutinefunction(callback):
                stop = await callback(job_key, status)
            else:
                stop = callback(job_key, status)
            if stop:
                break

    async def finish_abort(self, job: Job) -> None:
        await self.redis.delete(job.abort_id)
        await super().finish_abort(job)

    async def write_worker_info(
        self,
        worker_id: str,
        info: WorkerInfo,
        ttl: int,
    ) -> None:
        current = now()
        async with self.redis.pipeline(transaction=True) as pipe:
            key = self.namespace(f"worker_info:{worker_id}")
            await (
                pipe.setex(key, ttl, json.dumps(info))
                .zremrangebyscore(self._stats, 0, current)
                .zadd(self._stats, {key: current + millis(ttl)})
                .expire(self._stats, ttl)
                .execute()
            )

    async def _retry(self, job: Job, error: str | None) -> None:
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
            await self.notify(job)

    async def _finish(
        self,
        job: Job,
        status: Status,
        *,
        result: t.Any = None,
        error: str | None = None,
    ) -> None:
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

            await self.notify(job)

    async def _enqueue(self, job: Job) -> Job | None:
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

        async with self._op_sem:
            if not await self._enqueue_script(
                keys=[self._incomplete, job.id, self._queued, job.abort_id],
                args=[self.serialize(job), job.scheduled],
                client=self.redis,
            ):
                return None

        logger.info("Enqueuing %s", job.info(logger.isEnabledFor(logging.DEBUG)))
        return job


class PubSubMultiplexer(Multiplexer):
    """
    Handle multiple in-process channels over a single Redis channel.

    We use pubsub for realtime job updates. Each pubsub instance needs
    a connection, and that connection can't be reused when its done (that's
    an assumption, based on redis-py not releasing the connection and
    this comment: https://github.com/redis/go-redis/issues/785#issuecomment-394596158).
    So we use a single pubsub instance that listens to all job changes on
    the queue and handle message routing in-process.
    """

    def __init__(self, pubsub: PubSub, prefix: str) -> None:
        super().__init__()
        self.prefix = prefix
        self.pubsub = pubsub

    async def _start(self) -> None:
        await self.pubsub.psubscribe(f"{self.prefix}*")

        while True:
            try:
                message = await self.pubsub.get_message(timeout=None)  # type: ignore
                if message and message["type"] == "pmessage":
                    message["channel"] = message["channel"].decode("utf-8")
                    self.publish(message["channel"], message)
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Failed to consume message")

    async def _close(self) -> None:
        await self.pubsub.punsubscribe()
