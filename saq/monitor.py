import json

from saq import Queue, Job
from saq.queue import STATS_KEY
from saq.utils import now


class ImpersonatorQueue(Queue):
    """
    Queue that can read jobs from other queues.

    This little hack is intended solely for monitoring purposes.
    """

    def impersonate(self, name):
        return impersonate(queue=self, new_name=name)

    def _init_job(self, job_dict):
        queue_name = job_dict.pop("queue")
        queue = self.impersonate(name=queue_name)
        return Job(**job_dict, queue=queue)


def impersonate(queue, new_name="impersonator"):
    return ImpersonatorQueue(
        redis=queue.redis,
        name=new_name,
        dump=queue.dump,
        load=queue.load,
    )


def _transform_in_place(dictionary, key, transform):
    if key in dictionary:
        dictionary[key] = transform(dictionary[key])


def json_safe_job(job):
    job_dict = job.to_dict()
    _transform_in_place(job_dict, "kwargs", repr)
    _transform_in_place(job_dict, "result", repr)
    return job_dict


class Monitor:
    """
    Monitor to inspect all active queues.

    queue: queue from which to steal a connection to redis
    """

    @classmethod
    def from_url(cls, url):
        """Create a queue with a redis url a name."""
        return cls(ImpersonatorQueue.from_url(url))

    def __init__(self, queue):
        self.redis = queue.redis
        self.queue = impersonate(queue=queue)

    async def info(self, queue=None, jobs=False, offset=0, limit=10):
        # pylint: disable=too-many-locals
        queues = {}
        keys = []

        for key in await self.redis.zrangebyscore(STATS_KEY, now(), "inf"):
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

        for name, queue_info in queues.items():
            queue = self.queue.impersonate(name=name)
            queued = await queue.count("queued")
            active = await queue.count("active")
            incomplete = await queue.count("incomplete")

            jobs = (
                [
                    json_safe_job(queue.deserialize(job_bytes))
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

            queue_info.update(
                {
                    "name": name,
                    "queued": queued,
                    "active": active,
                    "scheduled": incomplete - queued - active,
                    "jobs": jobs,
                }
            )

        return queues

    async def job(self, job_key):
        return await self.queue.job(Job.id_from_key(job_key))

    async def disconnect(self):
        await self.queue.disconnect()
