import asyncio
import dataclasses
import json
import logging

import aioredis
from saq.job import Job, Status
from saq.utils import now, seconds, uuid1


logger = logging.getLogger("saq")


class Queue:
    """
    Queue is used to interact with aioredis.

    redis: instance of aioredis pool
    name: name of the queue
    dump: lambda that takes a dictionary and outputs bytes (default json.dumps)
    load: lambda that takes bytes and outputs a python dictionary (default json.loads)
    """

    @classmethod
    def from_url(cls, url, name="default"):
        """Create a queue with a redis url a name."""
        return cls(aioredis.from_url(url), name)

    def __init__(self, redis, name="default", dump=None, load=None):
        self.redis = redis
        self.name = name
        self.uuid = uuid1()
        self.started = now()
        self.complete = 0
        self.failed = 0
        self.retried = 0
        self.aborted = 0
        self._fields = {f.name: f.default for f in dataclasses.fields(Job)}
        self._version = None
        self._schedule_script = None
        self._enqueue_script = None
        self._cleanup_script = None
        self._incomplete = self.namespace("incomplete")
        self._queued = self.namespace("queued")
        self._active = self.namespace("active")
        self._schedule = self.namespace("schedule")
        self._sweep = self.namespace("sweep")
        self._dump = dump or json.dumps
        self._load = load or json.loads

    def namespace(self, key):
        return ":".join(["saq", self.name, key])

    def serialize(self, job):
        return self._dump(
            {
                k: v.name if k == "queue" else v
                for k, v in job.__dict__.items()
                if v != self._fields[k]
            }
        )

    def deserialize(self, job_bytes):
        if not job_bytes:
            return None

        job_dict = self._load(job_bytes)
        assert (
            job_dict.pop("queue") == self.name
        ), f"Job {job_dict} fetched by wrong queue: {self.name}"
        return Job(**job_dict, queue=self)

    async def version(self):
        if not self._version:
            info = await self.redis.info()
            self._version = tuple(int(i) for i in info["redis_version"].split("."))
        return self._version

    async def stats(self, ttl=60):
        stats = {
            "complete": self.complete,
            "failed": self.failed,
            "retried": self.retried,
            "aborted": self.aborted,
            "uptime": seconds(now() - self.started),
        }
        await self.redis.setex(
            self.namespace(f"stats:{self.uuid}"),
            ttl,
            self._dump(stats),
        )
        return stats

    async def count(self, kind):
        if kind == "queued":
            return await self.redis.llen(self._queued)
        if kind == "active":
            return await self.redis.llen(self._active)
        if kind == "incomplete":
            return await self.redis.zcard(self._incomplete)
        raise ValueError("Can't count unknown type {kind}")

    async def schedule(self, lock=1):
        self._schedule_script = self.redis.register_script(
            """
            if redis.call('EXISTS', KEYS[1]) == 0 then
                redis.call('SETEX', KEYS[1], ARGV[1], 1)
                local jobs = redis.call('ZRANGE', KEYS[2], 1, ARGV[2], 'BYSCORE')

                if next(jobs) then
                    local scores = {}
                    for _, v in ipairs(jobs) do
                        table.insert(scores, 0)
                        table.insert(scores, v)
                    end
                    redis.call('ZADD', KEYS[2], unpack(scores))
                    redis.call('RPUSH', KEYS[3], unpack(jobs))
                end

                return jobs
            end
            """
        )

        return await self._schedule_script(
            keys=[self._schedule, self._incomplete, self._queued],
            args=[lock, now() // 1000],
        )

    async def sweep(self, lock=60):
        if not self._cleanup_script:
            self._cleanup_script = self.redis.register_script(
                """
                if redis.call('EXISTS', KEYS[1]) == 0 then
                    redis.call('SETEX', KEYS[1], ARGV[1], 1)
                    return redis.call('LRANGE', KEYS[2], 0, -1)
                end
                """
            )

        logger.info("Sweeping stuck jobs")

        job_ids = await self._cleanup_script(
            keys=[self._sweep, self._active], args=[lock], client=self.redis
        )

        swept = []
        if job_ids:
            for job_id, job_bytes in zip(job_ids, await self.redis.mget(job_ids)):
                job = self.deserialize(job_bytes)

                if not job:
                    swept.append(job_id)
                    await self.redis.lrem(self._active, 0, job_id)
                elif job.stuck:
                    swept.append(job_id)
                    await self.abort(job, error="sweeped")
        return swept

    async def update(self, job):
        job.touched = now()
        await self.redis.set(job.job_id, self.serialize(job))

    async def job(self, job_id):
        return self.deserialize(await self.redis.get(job_id))

    async def abort(self, job, error="aborted"):
        await self.redis.lrem(self._queued, 0, job.job_id)
        await self.finish(job, Status.ABORTED, error=error)

    async def retry(self, job, error):
        try:
            job_id = job.job_id
            job.status = Status.QUEUED
            job.error = error
            job.completed = 0
            job.started = 0

            async with self.redis.pipeline(transaction=True) as pipe:
                await (
                    pipe.lrem(self._active, 1, job_id)
                    .rpush(self._queued, job_id)
                    .set(job_id, self.serialize(job))
                    .execute()
                )
                self.retried += 1
                logger.info("Retrying %s", job)
        except asyncio.CancelledError:
            pass

    async def finish(self, job, status, *, result=None, error=None):
        try:
            job_id = job.job_id
            job.status = status
            job.result = result
            job.error = error
            job.completed = now()

            async with self.redis.pipeline(transaction=True) as pipe:
                pipe = pipe.lrem(
                    self._active, 0 if status == Status.ABORTED else 1, job_id
                ).zrem(self._incomplete, job_id)

                if job.ttl:
                    pipe = pipe.setex(job_id, job.ttl, self.serialize(job))
                else:
                    pipe = pipe.set(job_id, self.serialize(job))

                await pipe.execute()

                if status == Status.COMPLETE:
                    self.complete += 1
                elif status == Status.FAILED:
                    self.failed += 1
                elif status == Status.ABORTED:
                    self.aborted += 1

                logger.info("Finished %s", job)
        except asyncio.CancelledError:
            pass

    async def dequeue(self, timeout=0):
        if await self.version() < (6, 2, 0):
            job_id = await self.redis.brpoplpush(self._queued, self._active, timeout)
        else:
            job_id = await self.redis.execute_command(
                "BLMOVE", self._queued, self._active, "RIGHT", "LEFT", timeout
            )
        return await self.job(job_id)

    async def enqueue(self, job_or_func, **kwargs):
        job_kwargs = {}

        for k, v in kwargs.items():
            if k in self._fields:
                job_kwargs[k] = v
            else:
                if "kwargs" not in job_kwargs:
                    job_kwargs["kwargs"] = {}
                job_kwargs["kwargs"][k] = v

        if isinstance(job_or_func, str):
            job = Job(function=job_or_func, **job_kwargs)
        else:
            job = dataclasses.replace(job_or_func, **job_kwargs)

        if job.queue and job.queue.name != self.name:
            raise ValueError(f"Job {job} registered to a different queue")

        if not self._enqueue_script:
            self._enqueue_script = self.redis.register_script(
                """
                if not redis.call('ZSCORE', KEYS[1], KEYS[2]) then
                    redis.call('SET', KEYS[2], ARGV[1])
                    redis.call('ZADD', KEYS[1], ARGV[2], KEYS[2])
                    if ARGV[2] == '0' then redis.call('RPUSH', KEYS[3], KEYS[2]) end
                    return ARGV[1]
                else
                    return redis.call('GET', KEYS[2])
                end
                """
            )

        job.queue = self
        job.enqueued = now()
        job.status = Status.QUEUED

        logger.info("Enqueuing %s", job)

        return self.deserialize(
            await self._enqueue_script(
                keys=[self._incomplete, job.job_id, self._queued],
                args=[self.serialize(job), job.scheduled],
                client=self.redis,
            )
        )
