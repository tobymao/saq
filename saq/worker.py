import asyncio
import logging
import signal
import traceback

from saq.job import Status
from saq.queue import Queue
from saq.utils import now


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
        monitor: how often to check if a job is aborted or to update it's heartbeat
    """

    SIGNALS = [signal.SIGINT, signal.SIGTERM]

    def __init__(
        self,
        queue,
        functions,
        *,
        concurrency=10,
        startup=None,
        shutdown=None,
        before_process=None,
        after_process=None,
        timers=None,
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
            "monitor": 1,
            **(timers or {}),
        }
        self.event = asyncio.Event()
        self.functions = {}
        self.context = {"worker": self}
        self.tasks = set()

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

    async def upkeep(self):
        """Start various upkeep tasks async."""

        async def poll(func, sleep, arg=None):
            while not self.event.is_set():
                await func(arg or sleep)
                await asyncio.sleep(sleep)

        return [
            asyncio.create_task(poll(self.queue.schedule, self.timers["schedule"])),
            asyncio.create_task(poll(self.queue.sweep, self.timers["sweep"])),
            asyncio.create_task(
                poll(self.queue.stats, self.timers["stats"], self.timers["stats"] + 1)
            ),
        ]

    async def monitor(self, task, job_id):
        while self.timers["monitor"]:
            await asyncio.sleep(self.timers["monitor"])
            job = await self.queue.job(job_id)

            if not job or job.status == Status.ABORTED:
                logger.info("Aborting %s", job or job_id)
                task.cancel()
                return

            if job.heartbeat and self.timers["sweep"]:
                # touch the job only if we care about heartbeats
                # and if we use upkeep to sweep it up
                await job.update()

    async def process(self):
        job = None
        monitor = None

        try:
            job = await self.queue.dequeue()
            job_id = job.id
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

            if self.timers["monitor"]:
                monitor = asyncio.create_task(self.monitor(task, job_id))

            result = await asyncio.wait_for(task, job.timeout)

            await job.finish(Status.COMPLETE, result=result)
        except asyncio.CancelledError:
            if job:
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
            if monitor and not monitor.done():
                monitor.cancel()
                await asyncio.gather(monitor, return_exceptions=True)

            if (
                self.after_process
                and job
                and job.status in (Status.FAILED, Status.COMPLETE)
            ):
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
