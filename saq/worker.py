import asyncio
import logging
import signal
import traceback
import os

from croniter import croniter

from saq.job import Status
from saq.queue import Queue
from saq.utils import millis, now, seconds


logger = logging.getLogger("saq")


class Worker:
    """
    Worker is used to process and monitor jobs.

    queue: instance of saq.queue.Queue
    functions: list of async functions
    concurrency: number of jobs to process concurrently
    startup: async function to call on startup
    shutdown: async function to call on shutdown
    before_process: async function to call before a job processes
    before_process: async function to call after a job processes
    timers: dict with various timer overrides in seconds
        schedule: how often we poll to schedule jobs
        stats: how often to update stats
        sweep: how often to cleanup stuck jobs
        abort: how often to check if a job is aborted
    """

    SIGNALS = [signal.SIGINT, signal.SIGTERM] if os.name != "nt" else [signal.SIGTERM]

    def __init__(
        self,
        queue,
        functions,
        *,
        concurrency=10,
        cron_jobs=None,
        startup=None,
        shutdown=None,
        before_process=None,
        after_process=None,
        timers=None,
        dequeue_timeout=0,
    ):
        self.queue = queue
        self.concurrency = concurrency
        self.startup = startup
        self.shutdown = shutdown
        self.before_process = before_process
        self.after_process = after_process
        self.timers = {
            "schedule": 1,
            "stats": 10,
            "sweep": 60,
            "abort": 1,
            **(timers or {}),
        }
        self.event = asyncio.Event()
        functions = set(functions)
        self.functions = {}
        self.cron_jobs = cron_jobs or []
        self.context = {"worker": self}
        self.tasks = set()
        self.job_task_contexts = {}
        self.dequeue_timeout = dequeue_timeout

        for job in self.cron_jobs:
            if not croniter.is_valid(job.cron):
                raise ValueError(f"Cron is invalid {job.cron}")
            functions.add(job.function)

        for function in functions:
            if isinstance(function, tuple):
                name, function = function
            else:
                name = function.__qualname__

            assert asyncio.iscoroutinefunction(
                function
            ), f"{function} is not a coroutine"

            self.functions[name] = function

    async def start(self):
        """Start processing jobs and upkeep tasks."""
        try:
            self.event = asyncio.Event()
            loop = asyncio.get_running_loop()

            for signum in self.SIGNALS:
                loop.add_signal_handler(signum, self.event.set)

            if self.startup:
                await self.startup(self.context)

            self.tasks.update(await self.upkeep())

            for _ in range(self.concurrency):
                self._process()

            await self.event.wait()
        finally:
            logger.info("Shutting down")

            if self.shutdown:
                await self.shutdown(self.context)

            await self.stop()

    async def stop(self):
        """Stop the worker and cleanup."""
        self.event.set()
        all_tasks = list(self.tasks)
        self.tasks.clear()
        for task in all_tasks:
            task.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)

    async def schedule(self, lock=1):
        for cron_job in self.cron_jobs:
            kwargs = cron_job.__dict__.copy()
            function = kwargs.pop("function").__qualname__
            kwargs["key"] = f"cron:{function}" if kwargs.pop("unique") else None
            scheduled = croniter(kwargs.pop("cron"), seconds(now())).get_next()

            await self.queue.enqueue(
                function,
                scheduled=int(scheduled),
                **{k: v for k, v in kwargs.items() if v is not None},
            )

        scheduled = await self.queue.schedule(lock)

        if scheduled:
            logger.info("Scheduled %s", scheduled)

    async def upkeep(self):
        """Start various upkeep tasks async."""

        async def poll(func, sleep, arg=None):
            while not self.event.is_set():
                await func(arg or sleep)
                await asyncio.sleep(sleep)

        return [
            asyncio.create_task(poll(self.abort, self.timers["abort"])),
            asyncio.create_task(poll(self.schedule, self.timers["schedule"])),
            asyncio.create_task(poll(self.queue.sweep, self.timers["sweep"])),
            asyncio.create_task(
                poll(self.queue.stats, self.timers["stats"], self.timers["stats"] + 1)
            ),
        ]

    async def abort(self, abort_threshold):
        jobs = [
            job
            for job in self.job_task_contexts
            if job.duration("running") >= millis(abort_threshold)
        ]

        if not jobs:
            return

        aborted = await self.queue.redis.mget(job.abort_id for job in jobs)

        for job, abort in zip(jobs, aborted):
            if not abort:
                continue

            task_data = self.job_task_contexts.get(job, {})
            task = task_data.get("task")

            if task and not task.done():
                task_data["aborted"] = True
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

            await job.finish(Status.ABORTED, error=abort.decode("utf-8"))
            await self.queue.redis.delete(job.abort_id)
            logger.info("Aborting %s", job.id)

    async def process(self):
        # pylint: disable=too-many-branches
        job = None

        try:
            job = await self.queue.dequeue(self.dequeue_timeout)

            if not job:
                return

            job.started = now()
            job.status = Status.ACTIVE
            job.attempts += 1
            logger.info("Processing %s", job)
            await job.update()
            context = {**self.context, "job": job}

            if self.before_process:
                await self.before_process(context)

            task = asyncio.create_task(
                self.functions[job.function](context, **(job.kwargs or {}))
            )
            self.job_task_contexts[job] = {"task": task, "aborted": False}
            result = await asyncio.wait_for(task, job.timeout)
            await job.finish(Status.COMPLETE, result=result)
        except asyncio.CancelledError:
            if job and not self.job_task_contexts.get(job, {}).get("aborted"):
                await job.retry("cancelled")
        except Exception:
            error = traceback.format_exc()
            logger.error(error)

            if job:
                if job.attempts >= job.retries:
                    await job.finish(Status.FAILED, error=error)
                else:
                    await job.retry(error)
        finally:
            if job:
                self.job_task_contexts.pop(job)

                if self.after_process and job.completed:
                    await self.after_process(context)

    def _process(self, previous_task=None):
        if previous_task:
            self.tasks.discard(previous_task)

        if not self.event.is_set():
            new_task = asyncio.create_task(self.process())
            self.tasks.add(new_task)
            new_task.add_done_callback(self._process)


def import_settings(settings):
    import importlib

    # given a.b.c, parses out a.b as the module path and c as the variable
    module_path, name = settings.strip().rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, name)


def start(settings, web=False, port=8080):
    settings = import_settings(settings)

    if "queue" not in settings:
        settings["queue"] = Queue.from_url("redis://localhost")

    loop = asyncio.new_event_loop()
    worker = Worker(**settings)

    if web:
        import aiohttp
        from saq.web import app

        async def shutdown(_app):
            await worker.stop()
            await worker.queue.disconnect()

        app.on_shutdown.append(shutdown)
        app["queue"] = worker.queue
        loop.create_task(worker.start())
        aiohttp.web.run_app(app, port=port, loop=loop)
    else:
        loop.run_until_complete(worker.start())


async def async_check_health(queue: Queue) -> int:
    info = await queue.info()
    if not info.get(queue.name):
        logger.warning("Health check failed")
        status = 1
    else:
        logger.info(info[queue.name])
        status = 0
    await queue.disconnect()
    return status


def check_health(settings: str) -> int:
    settings = import_settings(settings)
    loop = asyncio.new_event_loop()
    queue = settings.get("queue", Queue.from_url("redis://localhost"))
    return loop.run_until_complete(async_check_health(queue))
