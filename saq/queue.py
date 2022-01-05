import asyncio
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
        self._version = None
        self._schedule_script = None
        self._enqueue_script = None
        self._cleanup_script = None
        self._stats = "saq:stats"
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
        return self._dump(job.to_dict())

    def deserialize(self, job_bytes):
        if not job_bytes:
            return None

        job_dict = self._load(job_bytes)
        assert (
            job_dict.pop("queue") == self.name
        ), f"Job {job_dict} fetched by wrong queue: {self.name}"
        return Job(**job_dict, queue=self)

    async def disconnect(self):
        await self.redis.close()
        await self.redis.connection_pool.disconnect()

    async def version(self):
        if not self._version:
            info = await self.redis.info()
            self._version = tuple(int(i) for i in info["redis_version"].split("."))
        return self._version

    async def info(self, queue=None, jobs=False, offset=0, limit=10):
        # pylint: disable=too-many-locals
        queues = {}
        keys = []

        for key in await self.redis.zrangebyscore(self._stats, now(), "inf"):
            key = key.decode("utf-8")
            _, name, _, worker = key.split(":")
            queues[name] = {"workers": {}}
            keys.append((name, worker))

        worker_stats = await self.redis.mget(
            f"saq:{name}:stats:{worker}" for name, worker in keys
        )

        for (name, worker), stats in zip(keys, worker_stats):
            if stats:
                stats = json.loads(stats.decode("UTF-8"))
                queues[name]["workers"][worker] = stats

        for name in queues:
            queue = Queue(self.redis, name=name)
            queued = await queue.count("queued")
            active = await queue.count("active")
            incomplete = await queue.count("incomplete")
            jobs = (
                [
                    queue.deserialize(job_bytes).to_dict()
                    for job_bytes in await self.redis.mget(
                        (
                            await self.redis.lrange(
                                queue.namespace("active"), offset, limit - 1
                            )
                        )
                        + (
                            await self.redis.lrange(
                                queue.namespace("queued"), offset, limit - 1
                            )
                        )
                    )
                ]
                if jobs
                else []
            )
            scheduled = incomplete - queued - active

            queues[name] = {
                "name": name,
                "queued": queued,
                "active": active,
                "scheduled": scheduled,
                "jobs": jobs,
                **queues[name],
            }

        return queues

    async def stats(self, ttl=60):
        current = now()
        stats = {
            "complete": self.complete,
            "failed": self.failed,
            "retried": self.retried,
            "aborted": self.aborted,
            "uptime": current - self.started,
        }
        async with self.redis.pipeline(transaction=True) as pipe:
            key = self.namespace(f"stats:{self.uuid}")
            await (
                pipe.setex(key, ttl, json.dumps(stats))
                .zremrangebyscore(self._stats, 0, current)
                .zadd(self._stats, {key: current + ttl * 1000})
                .execute()
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
            args=[lock, seconds(now())],
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
                elif job.status != Status.ACTIVE or job.stuck:
                    swept.append(job_id)
                    await self.abort(job, error="sweeped")
        return swept

    async def listen(self, job, callback, timeout=10):
        """
        Listen to updates on job.

        job: job instance
        callback: callback function, if it returns truthy, break
        timeout: if timeout is truthy, wait for timeout seconds
        """
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(job.id)

        async def listen():
            async for message in pubsub.listen():
                if message["type"] == "message":
                    job_id = message["channel"].decode("utf-8")
                    status = Status[message["data"].decode("utf-8").upper()]
                    if asyncio.iscoroutinefunction(callback):
                        stop = await callback(job_id, status)
                    else:
                        stop = callback(job_id, status)

                    if stop:
                        break

        try:
            if timeout:
                await asyncio.wait_for(listen(), timeout)
            else:
                await listen()
        finally:
            await pubsub.unsubscribe(job.id)

    async def notify(self, job):
        await self.redis.publish(job.id, job.status)

    async def update(self, job):
        job.touched = now()
        await self.redis.set(job.id, self.serialize(job))
        await self.notify(job)

    async def job(self, job_id):
        return self.deserialize(await self.redis.get(job_id))

    async def abort(self, job, error):
        await self.redis.lrem(self._queued, 0, job.id)
        await self.finish(job, Status.ABORTED, error=error)

    async def retry(self, job, error):
        job_id = job.id
        job.status = Status.QUEUED
        job.error = error
        job.completed = 0
        job.started = 0
        job.progress = 0
        job.touched = now()

        async with self.redis.pipeline(transaction=True) as pipe:
            await (
                pipe.lrem(self._active, 1, job_id)
                .lrem(self._queued, 1, job_id)
                .zadd(self._incomplete, {job_id: job.scheduled})
                .rpush(self._queued, job_id)
                .set(job_id, self.serialize(job))
                .execute()
            )
            self.retried += 1
            await self.notify(job)
            logger.info("Retrying %s", job)

    async def finish(self, job, status, *, result=None, error=None):
        job_id = job.id
        job.status = status
        job.result = result
        job.error = error
        job.completed = now()

        if status == Status.COMPLETE:
            job.progress = 1.0

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

            await self.notify(job)
            logger.info("Finished %s", job)

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
            if k in Job.__dataclass_fields__:  # pylint: disable=no-member
                job_kwargs[k] = v
            else:
                if "kwargs" not in job_kwargs:
                    job_kwargs["kwargs"] = {}
                job_kwargs["kwargs"][k] = v

        if isinstance(job_or_func, str):
            job = Job(function=job_or_func, **job_kwargs)
        else:
            job = job_or_func

            for k, v in job_kwargs.items():
                setattr(job, k, v)

        if job.queue and job.queue.name != self.name:
            raise ValueError(f"Job {job} registered to a different queue")

        if not self._enqueue_script:
            self._enqueue_script = self.redis.register_script(
                """
                if not redis.call('ZSCORE', KEYS[1], KEYS[2]) then
                    redis.call('SET', KEYS[2], ARGV[1])
                    redis.call('ZADD', KEYS[1], ARGV[2], KEYS[2])
                    if ARGV[2] == '0' then redis.call('RPUSH', KEYS[3], KEYS[2]) end
                end
                """
            )

        job.queue = self
        job.queued = now()
        job.status = Status.QUEUED

        logger.info("Enqueuing %s", job)

        await self._enqueue_script(
            keys=[self._incomplete, job.id, self._queued],
            args=[self.serialize(job), job.scheduled],
            client=self.redis,
        )
        await job.refresh()
        return job
