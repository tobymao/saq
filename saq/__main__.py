import argparse
import asyncio
import importlib
import logging
import multiprocessing

from saq.queue import Queue
from saq.worker import Worker


def start(options, web=False):
    if "queue" not in settings:
        settings["queue"] = Queue.from_url("redis://localhost")

    loop = asyncio.new_event_loop()
    worker = Worker(**options)
    loop.create_task(worker.start())

    if web:
        import aiohttp
        from saq.web import app

        async def shutdown(_app):
            await worker.stop()

        app.on_shutdown.append(shutdown)
        aiohttp.web.run_app(app, loop=loop)
    else:
        loop.run_forever()


parser = argparse.ArgumentParser(description="Start Simple Async Queue Worker")
parser.add_argument(
    "settings",
    type=str,
    help="Namespaced variable containing worker settings eg: eg module_a.settings",
)
parser.add_argument("--workers", type=int, help="Number of worker processes", default=1)
parser.add_argument(
    "--verbose",
    "-v",
    action="count",
    help="Logging level: 0: ERROR, 1: INFO, 2: DEBUG",
    default=0,
)
parser.add_argument(
    "--web",
    action="store_true",
    help="Start web app",
)

args = parser.parse_args()

# given a.b.c, parses out a.b as the  module path and c as the variable
module_path, name = args.settings.strip().rsplit(".", 1)
module = importlib.import_module(module_path)
settings = getattr(module, name)

level = args.verbose

if level == 0:
    level = logging.ERROR
elif level == 1:
    level = logging.INFO
else:
    level = logging.DEBUG

logging.basicConfig(level=level)

workers = args.workers

if workers > 1:
    for _ in range(workers - 1):
        p = multiprocessing.Process(target=start, args=(settings))
        p.start()

start(settings, web=args.web)
