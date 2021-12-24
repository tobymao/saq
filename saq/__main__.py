import argparse
import asyncio
import importlib
import logging
import multiprocessing

from saq.queue import Queue
from saq.worker import Worker


def start(options):
    if "queue" not in settings:
        settings["queue"] = Queue.from_url("redis://localhost")

    loop = asyncio.new_event_loop()
    worker = Worker(**options)
    loop.create_task(worker.start())
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

args = parser.parse_args()

# given a.b.c, parses out a.b as the  module path and c as the variable
module_path, name = args.settings.strip().rsplit(".", 1)
module = importlib.import_module(module_path)
settings = getattr(module, name)

level = args.verbose
logging.basicConfig()
logger = logging.getLogger("saq")

if level == 0:
    logger.setLevel(logging.ERROR)
elif level == 1:
    logger.setLevel(logging.INFO)
else:
    logger.setLevel(logging.DEBUG)

workers = args.workers

if workers > 1:
    for _ in range(workers):
        p = multiprocessing.Process(target=start, args=(settings,))
        p.start()
else:
    start(settings)
