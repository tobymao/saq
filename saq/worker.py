import asyncio
import logging
import signal
import traceback

from saq.job import Status
from saq.utils import now


logger = logging.getLogger("saq")


class Worker:
    SIGNALS = [signal.SIGINT, signal.SIGTERM]

    def __init__(
        self,
        queue,
        functions,
        *,
        concurrency=5,
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
        try:
            self.event.clear()
            loop = asyncio.get_running_loop()

            for signum in self.SIGNALS:
                loop.add_signal_handler(
                    signum, lambda: loop.create_task(self.handle_signal())
                )

            if self.startup:
                await self.startup(self.context)

            await self.upkeep()

            for _ in range(self.concurrency):
                self._process()

            await self.event.wait()
        finally:
            logger.info("Shutting down")
            if self.shutdown:
                await self.shutdown(self.context)

    async def stop(self):
        """Stop the worker and cleanup."""
        self.event.set()
        await self.queue.disconnect()
        for task in asyncio.all_tasks(asyncio.get_running_loop()):
            if task is not asyncio.current_task():
                task.cancel()
                try:
                    await task
                except (Exception, asyncio.CancelledError):
                    pass

    async def handle_signal(self):
        logger.info("Received signal")
        await self.stop()
        asyncio.get_running_loop().stop()

    async def upkeep(self):
        async def poll(func, sleep, arg=None):
            while not self.event.is_set():
                await func(arg or sleep)
                await asyncio.sleep(sleep)

        asyncio.create_task(poll(self.queue.schedule, self.timers["schedule"]))
        asyncio.create_task(poll(self.queue.sweep, self.timers["sweep"]))
        asyncio.create_task(
            poll(self.queue.stats, self.timers["stats"], self.timers["stats"] + 1)
        )

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
                await self.queue.update(job)

    async def process(self):
        job = await self.queue.dequeue()
        job_id = job.id
        job.started = now()
        job.status = Status.ACTIVE
        job.attempts += 1
        logger.info("Processing %s", job)
        await self.queue.update(job)
        monitor = None
        context = {**self.context, "job": job}

        try:
            if self.before_process:
                await self.before_process(context)

            task = asyncio.create_task(
                self.functions[job.function](context, **(job.kwargs or {}))
            )

            if self.timers["monitor"]:
                monitor = asyncio.create_task(self.monitor(task, job_id))

            result = await asyncio.wait_for(task, job.timeout)

            if self.after_process:
                await self.after_process(context)

            await self.queue.finish(job, Status.COMPLETE, result=result)
        except asyncio.CancelledError:
            job = await self.queue.job(job_id)
            if job and job.status == Status.ACTIVE:
                await self.queue.retry(job, "cancelled")
        except Exception:
            error = traceback.format_exc()
            logger.error(error)

            if job.attempts >= job.retries:
                await self.queue.finish(job, Status.FAILED, error=error)
            else:
                await self.queue.retry(job, error)
        finally:
            if monitor and not monitor.done():
                monitor.cancel()

    def _process(self):
        if not self.event.is_set():
            asyncio.create_task(self.process()).add_done_callback(
                lambda _: self._process()
            )
