import argparse
import logging
import multiprocessing
import sys

from saq.worker import check_health, start


def main():
    parser = argparse.ArgumentParser(description="Start Simple Async Queue Worker")
    parser.add_argument(
        "settings",
        type=str,
        help="Namespaced variable containing worker settings eg: eg module_a.settings",
    )
    parser.add_argument(
        "--workers", type=int, help="Number of worker processes", default=1
    )
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
    parser.add_argument(
        "--port",
        type=str,
        help="Web app port, defaults to 8080",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Perform a health check",
    )

    args = parser.parse_args()

    level = args.verbose

    if level == 0:
        level = logging.ERROR
    elif level == 1:
        level = logging.INFO
    else:
        level = logging.DEBUG

    settings = args.settings
    logging.basicConfig(level=level)

    if args.check:
        sys.exit(check_health(settings))
    else:
        workers = args.workers

        if workers > 1:
            for _ in range(workers - 1):
                p = multiprocessing.Process(target=start, args=(settings,))
                p.start()

        start(settings, web=args.web, port=args.port)


if __name__ == "__main__":
    main()
