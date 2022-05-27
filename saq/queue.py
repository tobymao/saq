import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager

from redis import asyncio as aioredis

from saq.job import (
    Job,
    Status,
    TERMINAL_STATUSES,
    UNSUCCESSFUL_TERMINAL_STATUSES,
    get_default_job_key,
)
from saq.utils import (
    millis,
    now,
    seconds,
    uuid1,
)


logger = logging.getLogger("saq")

ID_PREFIX = "saq:job:"


class JobError(Exception):
    def __init__(self, job):
        super().__init__(
            f"Job {job.id} {job.status}\n\nThe above job failed with the following error:\n\n{job.error}"
        )
        self.job = job


class Queue:
    """
    Queue is used to interact with aioredis.

    redis: instance of aioredis pool
    name: name of the queue
    dump: lambda that takes a dictionary and outputs bytes (default json.dumps)
    load: lambda that takes bytes and outputs a python dictionary (default json.loads)
    max_concurrent_ops: maximum concurrent operations.
        This throttles calls to `enqueue`, `job`, and `abort` to prevent the Queue
        from consuming too many Redis connections.
    """

    @classmethod
    def from_url(cls, url, **kwargs):
        """Create a queue with a redis url a name."""
        return cls(aioredis.from_url(url), **kwargs)

    def __init__(  # pylint: disable=too-many-arguments
        self,
        redis,
        name="default",
        dump=None,
        load=None,
        max_concurrent_ops=20,
    ):
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
        self._incomplete = self.namespace("incomplete")
        self._queued = self.namespace("queued")
        self._active = self.namespace("active")
        self._schedule = self.namespace("schedule")
        self._sweep = self.namespace("sweep")
        self._stats = self.namespace("stats")
        self._dump = dump or json.dumps
        self._load = load or json.loads
        self._before_enqueues = {}
        self._op_sem = asyncio.Semaphore(max_concurrent_ops)

    def register_before_enqueue(self, callback):
        self._before_enqueues[id(callback)] = callback

    def unregister_before_enqueue(self, callback):
        self._before_enqueues.pop(id(callback), None)

    def job_id(self, job_key):
        return f"{ID_PREFIX}{self.name}:{job_key}"

    async def _before_enqueue(self, job):
        for cb in self._before_enqueues.values():
            await cb(job)

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

    async def info(self, jobs=False, offset=0, limit=10):
        # pylint: disable=too-many-locals
        worker_uuids = []

        for key in await self.redis.zrangebyscore(self._stats, now(), "inf"):
            key = key.decode("utf-8")
            *_, worker_uuid = key.split(":")
            worker_uuids.append(worker_uuid)

        worker_stats = await self.redis.mget(
            self.namespace(f"stats:{worker_uuid}") for worker_uuid in worker_uuids
        )

        worker_info = {}
        for worker_uuid, stats in zip(worker_uuids, worker_stats):
            if stats:
                stats = json.loads(stats.decode("UTF-8"))
                worker_info[worker_uuid] = stats

        queued = await self.count("queued")
        active = await self.count("active")
        incomplete = await self.count("incomplete")

        if jobs:
            job_info = [
                self.deserialize(job_bytes).to_dict()
                for job_bytes in await self.redis.mget(
                    (
                        await self.redis.lrange(
                            self.namespace("active"), offset, limit - 1
                        )
                    )
                    + (
                        await self.redis.lrange(
                            self.namespace("queued"), offset, limit - 1
                        )
                    )
                )
            ]
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
                .zadd(self._stats, {key: current + millis(ttl)})
                .expire(self._stats, ttl)
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
        if not self._schedule_script:
            self._schedule_script = self.redis.register_script(
                """
                if redis.call('EXISTS', KEYS[1]) == 0 then
                    redis.call('SETEX', KEYS[1], ARGV[1], 1)
                    local jobs = redis.call('ZRANGEBYSCORE', KEYS[2], 1, ARGV[2])

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

        job_ids = await self._cleanup_script(
            keys=[self._sweep, self._active], args=[lock], client=self.redis
        )

        swept = []
        if job_ids:
            for job_id, job_bytes in zip(job_ids, await self.redis.mget(job_ids)):
                job = self.deserialize(job_bytes)

                if not job:
                    swept.append(job_id)

                    async with self.redis.pipeline(transaction=True) as pipe:
                        await (
                            pipe.lrem(self._active, 0, job_id)
                            .zrem(self._incomplete, job_id)
                            .execute()
                        )
                    logger.info("Sweeping missing job %s", job_id)
                elif job.status != Status.ACTIVE or job.stuck:
                    swept.append(job_id)
                    await job.finish(Status.ABORTED, error="swept")
                    logger.info("Sweeping job %s", job)
        return swept

    async def listen(self, job_keys, callback, timeout=10):
        """
        Listen to updates on jobs.

        job_keys: sequence of job keys
        callback: callback function, if it returns truthy, break
        timeout: if timeout is truthy, wait for timeout seconds
        """
        pubsub = self.redis.pubsub()
        job_ids = [self.job_id(job_key) for job_key in job_keys]
        await pubsub.subscribe(*job_ids)

        async def listen():
            async for message in pubsub.listen():
                if message["type"] == "message":
                    job_id = message["channel"].decode("utf-8")
                    job_key = Job.key_from_id(job_id)
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
            await pubsub.unsubscribe(*job_ids)

    async def notify(self, job):
        await self.redis.publish(job.id, job.status)

    async def update(self, job):
        job.touched = now()
        await self.redis.set(job.id, self.serialize(job))
        await self.notify(job)

    async def job(self, job_key):
        job_id = self.job_id(job_key)
        return await self._get_job_by_id(job_id)

    async def _get_job_by_id(self, job_id):
        async with self._op_sem:
            return self.deserialize(await self.redis.get(job_id))

    async def abort(self, job, error, ttl=5):
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

    async def retry(self, job, error):
        job_id = job.id
        job.status = Status.QUEUED
        job.error = error
        job.completed = 0
        job.started = 0
        job.progress = 0
        job.touched = now()
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
            logger.info("Finished %s", job)

    async def dequeue(self, timeout=0):
        if await self.version() < (6, 2, 0):
            job_id = await self.redis.brpoplpush(self._queued, self._active, timeout)
        else:
            job_id = await self.redis.execute_command(
                "BLMOVE", self._queued, self._active, "RIGHT", "LEFT", timeout
            )
        if job_id is not None:
            return await self._get_job_by_id(job_id)

        logger.debug("Dequeue timed out")
        return None

    async def enqueue(self, job_or_func, **kwargs):
        """
        Enqueue a job by instance or string.

        Kwargs can be arguments of the function or properties of the job.
        If a job instance is passed in, it's properties are overriden.

        If the job has already been enqueued, this returns None.
        """
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

        job.queue = self
        job.queued = now()
        job.status = Status.QUEUED

        await self._before_enqueue(job)

        async with self._op_sem:
            if not await self._enqueue_script(
                keys=[self._incomplete, job.id, self._queued, job.abort_id],
                args=[self.serialize(job), job.scheduled],
                client=self.redis,
            ):
                return None

        logger.info("Enqueuing %s", job)
        return job

    async def apply(self, job_or_func, timeout=None, **kwargs):
        """
        Enqueue a job and wait for its result.

        If the job is successful, this returns its result.
        If the job is unsuccessful, this raises a JobError.

        Example::
            try:
                assert await queue.apply("add", a=1, b=2) == 3
            except JobError:
                print("job failed")

        job_or_func: Same as Queue.enqueue
        kwargs: Same as Queue.enqueue
        """
        results = await self.map(job_or_func, timeout=timeout, iter_kwargs=[kwargs])
        return results[0]

    async def map(
        self, job_or_func, iter_kwargs, timeout=None, return_exceptions=False, **kwargs
    ):
        """
        Enqueue multiple jobs and collect all of their results.

        Example::
            try:
                assert await queue.map(
                    "add",
                    [
                        {"a": 1, "b": 2},
                        {"a": 3, "b": 4},
                    ]
                ) == [3, 7]
            except JobError:
                print("any of the jobs failed")

        job_or_func: Same as Queue.enqueue
        iter_kwargs: Enqueue a job for each item in this sequence. Each item is the same
            as kwargs for Queue.enqueue.
        timeout: Total seconds to wait for all jobs to complete. If None (default) or 0, wait forever.
        return_exceptions: If False (default), an exception is immediately raised as soon as any jobs
            fail. Other jobs won't be cancelled and will continue to run.
            If True, exceptions are treated the same as successful results and aggregated in the result list.
        kwargs: Default kwargs for all jobs. These will be overridden by those in iter_kwargs.
        """
        iter_kwargs = [
            {
                "timeout": timeout,
                "key": kwargs.get("key") or get_default_job_key(),
                **kwargs,
                **kw,
            }
            for kw in iter_kwargs
        ]
        job_keys = [key["key"] for key in iter_kwargs]
        pending_job_keys = set(job_keys)

        async def callback(job_key, status):
            if status in TERMINAL_STATUSES:
                pending_job_keys.discard(job_key)

            if status in UNSUCCESSFUL_TERMINAL_STATUSES and not return_exceptions:
                return True

            if not pending_job_keys:
                return True

        # Start listening before we enqueue the jobs.
        # This ensures we don't miss any updates.
        task = asyncio.create_task(
            self.listen(pending_job_keys, callback, timeout=None)
        )

        try:
            await asyncio.gather(
                *[self.enqueue(job_or_func, **kw) for kw in iter_kwargs]
            )
        except:
            task.cancel()
            raise

        await asyncio.wait_for(task, timeout=timeout)

        jobs = await asyncio.gather(*[self.job(job_key) for job_key in job_keys])

        results = []
        for job in jobs:
            if job.status in UNSUCCESSFUL_TERMINAL_STATUSES:
                exc = JobError(job)
                if not return_exceptions:
                    raise exc
                results.append(exc)
            else:
                results.append(job.result)
        return results

    @asynccontextmanager
    async def batch(self):
        """
        Context manager to batch enqueue jobs.

        This tracks all jobs enqueued within the context manager scope and ensures that
        all are aborted if any exception is raised.

        Example::
            async with queue.batch():
                await queue.enqueue("test")  # This will get cancelled
                raise asyncio.CancelledError
        """
        children = set()

        async def track_child(job):
            children.add(job)

        self.register_before_enqueue(track_child)

        try:
            yield
        except:
            await asyncio.gather(
                *[self.abort(child, "cancelled") for child in children],
                return_exceptions=True,
            )
            raise
        finally:
            self.unregister_before_enqueue(track_child)
