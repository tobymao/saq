import argparse
import logging
import multiprocessing

from saq.worker import start


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

level = args.verbose

if level == 0:
    level = logging.ERROR
elif level == 1:
    level = logging.INFO
else:
    level = logging.DEBUG

logging.basicConfig(level=level)

settings = args.settings
workers = args.workers

if workers > 1:
    for _ in range(workers - 1):
        p = multiprocessing.Process(target=start, args=(settings,))
        p.start()

start(settings, web=args.web)
