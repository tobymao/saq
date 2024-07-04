"""
Workers
"""

from __future__ import annotations

import asyncio
import contextvars
import logging
import os
import signal
import traceback
import typing as t

from croniter import croniter

from saq.job import Status
from saq.queue import Queue
from saq.utils import millis, now, seconds

if t.TYPE_CHECKING:
    from asyncio import Task
    from collections.abc import Callable, Collection, Coroutine

    from aiohttp.web_app import Application

    from saq.job import CronJob, Job
    from saq.types import (
        Context,
        Function,
        JobTaskContext,
        PartialTimersDict,
        ReceivesContext,
        TimersDict,
    )


logger = logging.getLogger("saq")


class Worker:
    """
    Worker is used to process and monitor jobs.

    Args:
        queue: instance of saq.queue.Queue
        functions: list of async functions
        concurrency: number of jobs to process concurrently
        cron_jobs: List of CronJob instances.
        startup: async function to call on startup
        shutdown: async function to call on shutdown
        before_process: async function to call before a job processes
        after_process: async function to call after a job processes
        timers: dict with various timer overrides in seconds
            schedule: how often we poll to schedule jobs
            stats: how often to update stats
            sweep: how often to clean up stuck jobs
            abort: how often to check if a job is aborted
        dequeue_timeout: how long it will wait to dequeue
    """

    SIGNALS = [signal.SIGINT, signal.SIGTERM] if os.name != "nt" else [signal.SIGTERM]

    def __init__(
        self,
        queue: Queue,
        functions: Collection[Function | tuple[str, Function]],
        *,
        concurrency: int = 10,
        cron_jobs: Collection[CronJob] | None = None,
        startup: ReceivesContext | None = None,
        shutdown: ReceivesContext | None = None,
        before_process: ReceivesContext | None = None,
        after_process: ReceivesContext | None = None,
        timers: PartialTimersDict | None = None,
        dequeue_timeout: float = 0,
    ) -> None:
        self.queue = queue
        self.concurrency = concurrency
        self.startup = startup
        self.shutdown = shutdown
        self.before_process = before_process
        self.after_process = after_process
        self.timers: TimersDict = {
            "schedule": 1,
            "stats": 10,
            "sweep": 60,
            "abort": 1,
        }
        if timers is not None:
            self.timers.update(timers)
        self.event = asyncio.Event()
        functions = set(functions)
        self.functions: dict[str, Function] = {}
        self.cron_jobs: Collection[CronJob] = cron_jobs or []
        self.context: Context = {"worker": self}
        self.tasks: set[Task[t.Any]] = set()
        self.job_task_contexts: dict[Job, JobTaskContext] = {}
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

            self.functions[name] = function

    async def _before_process(self, ctx: Context) -> None:
        if self.before_process:
            await self.before_process(ctx)

    async def _after_process(self, ctx: Context) -> None:
        if self.after_process:
            await self.after_process(ctx)

    async def start(self) -> None:
        """Start processing jobs and upkeep tasks."""
        logger.info("Worker starting: %s", repr(self.queue))
        logger.debug(
            "Registered functions:\n%s", "\n".join(f"  {key}" for key in self.functions)
        )

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

            for signum in self.SIGNALS:
                loop.remove_signal_handler(signum)
        finally:
            logger.info("Worker shutting down")

            if self.shutdown:
                await self.shutdown(self.context)

            await self.stop()

    async def stop(self) -> None:
        """Stop the worker and cleanup."""
        self.event.set()
        all_tasks = list(self.tasks)
        self.tasks.clear()
        for task in all_tasks:
            task.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)

    async def schedule(self, lock: int = 1) -> None:
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

    async def upkeep(self) -> list[Task[None]]:
        """Start various upkeep tasks async."""

        async def poll(
            func: Callable[[int], Coroutine], sleep: int, arg: int | None = None
        ) -> None:
            while not self.event.is_set():
                try:
                    await func(arg or sleep)
                except (Exception, asyncio.CancelledError):
                    if self.event.is_set():
                        return
                    logger.exception("Upkeep task failed unexpectedly")

                await asyncio.sleep(sleep)

        return [
            asyncio.create_task(poll(self.abort, self.timers["abort"])),
            asyncio.create_task(poll(self.schedule, self.timers["schedule"])),
            asyncio.create_task(poll(self.queue.sweep, self.timers["sweep"])),
            asyncio.create_task(
                poll(self.queue.stats, self.timers["stats"], self.timers["stats"] + 1)
            ),
        ]

    async def abort(self, abort_threshold: float) -> None:
        def get_duration(job: Job) -> float:
            return job.duration("running") or 0

        jobs = [
            job
            for job in self.job_task_contexts
            if get_duration(job) >= millis(abort_threshold)
        ]

        if not jobs:
            return

        aborted = await self.queue.redis.mget(job.abort_id for job in jobs)

        for job, abort in zip(jobs, aborted):
            if not abort:
                continue

            task_data: JobTaskContext = self.job_task_contexts.get(job, {})
            task = task_data.get("task")

            if task and not task.done():
                task_data["aborted"] = abort.decode("utf-8")
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

            await self.queue.redis.delete(job.abort_id)
            logger.info("Aborting %s", job.id)

    async def process(self) -> None:
        # pylint: disable=too-many-branches
        context: Context | None = None
        job: Job | None = None

        try:
            job = await self.queue.dequeue(self.dequeue_timeout)

            if job is None:
                return

            job.started = now()
            job.status = Status.ACTIVE
            job.attempts += 1
            await job.update()
            context = {**self.context, "job": job}
            await self._before_process(context)
            logger.info("Processing %s", job.info(logger.isEnabledFor(logging.DEBUG)))

            function = ensure_coroutine_function(self.functions[job.function])
            task = asyncio.create_task(function(context, **(job.kwargs or {})))
            self.job_task_contexts[job] = {"task": task, "aborted": None}
            result = await asyncio.wait_for(task, job.timeout if job.timeout else None)
            await job.finish(Status.COMPLETE, result=result)
        except asyncio.CancelledError:
            if job:
                aborted = self.job_task_contexts.get(job, {}).get("aborted")
                if aborted:
                    await job.finish(Status.ABORTED, error=aborted)
                else:
                    await job.retry("cancelled")
        except Exception:
            logger.exception("Error processing job %s", job)

            if job:
                error = traceback.format_exc()

                if job.retryable:
                    await job.retry(error)
                else:
                    await job.finish(Status.FAILED, error=error)
        finally:
            if context:
                if job is not None:
                    self.job_task_contexts.pop(job, None)

                try:
                    await self._after_process(context)
                except (Exception, asyncio.CancelledError):
                    logger.exception("Failed to run after process hook")

    def _process(self, previous_task: Task | None = None) -> None:
        if previous_task:
            self.tasks.discard(previous_task)

        if not self.event.is_set():
            new_task = asyncio.create_task(self.process())
            self.tasks.add(new_task)
            new_task.add_done_callback(self._process)


def ensure_coroutine_function(func: Callable) -> Callable[..., Coroutine]:
    if asyncio.iscoroutinefunction(func):
        return func

    async def wrapped(*args: t.Any, **kwargs: t.Any) -> t.Any:
        loop = asyncio.get_running_loop()
        ctx = contextvars.copy_context()
        return await loop.run_in_executor(
            executor=None, func=lambda: ctx.run(func, *args, **kwargs)
        )

    return wrapped


def import_settings(settings: str) -> dict[str, t.Any]:
    import importlib

    # given a.b.c, parses out a.b as the module path and c as the variable
    module_path, name = settings.strip().rsplit(".", 1)
    module = importlib.import_module(module_path)
    settings_obj = getattr(module, name)

    if callable(settings_obj):
        settings_obj = settings_obj()

    return settings_obj


def start(
    settings: str,
    web: bool = False,
    extra_web_settings: list[str] | None = None,
    port: int = 8080,
) -> None:
    settings_obj = import_settings(settings)

    if "queue" not in settings_obj:
        settings_obj["queue"] = Queue.from_url("redis://localhost")

    loop = asyncio.new_event_loop()
    worker = Worker(**settings_obj)

    if web:
        import aiohttp.web

        from saq.web.aiohttp import create_app

        extra_web_settings = extra_web_settings or []
        web_settings = [settings_obj] + [import_settings(s) for s in extra_web_settings]
        queues = [s["queue"] for s in web_settings if s.get("queue")]

        async def shutdown(_app: Application) -> None:
            await worker.stop()

        app = create_app(queues)
        app.on_shutdown.append(shutdown)

        loop.create_task(worker.start())
        aiohttp.web.run_app(app, port=port, loop=loop)
    else:
        loop.run_until_complete(worker.start())


async def async_check_health(queue: Queue) -> int:
    info = await queue.info()
    name = info.get("name")
    if name != queue.name:
        logger.warning(
            "Health check failed. Unknown queue name %s. Expected %s",
            name,
            queue.name,
        )
        status = 1
    elif not info.get("workers"):
        logger.warning("No active workers found for queue %s", name)
        status = 1
    else:
        workers = len(info["workers"].values())
        logger.info("Found %d active workers for queue %s", workers, name)
        status = 0

    await queue.disconnect()
    return status


def check_health(settings: str) -> int:
    settings_dict = import_settings(settings)
    loop = asyncio.new_event_loop()
    queue = settings_dict.get("queue", Queue.from_url("redis://localhost"))
    return loop.run_until_complete(async_check_health(queue))
