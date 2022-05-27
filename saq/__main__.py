import argparse
import logging.config
import multiprocessing
import sys

from saq.worker import check_health, start
from saq.log import generate_logger_config


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
        help="Start web app. "
        "By default, this only monitors the current worker's queue. To monitor multiple queues, see '--extra-web-settings'",
    )
    parser.add_argument(
        "--extra-web-settings",
        "-e",
        action="append",
        help="Additional worker settings to monitor in the web app",
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

    logging.config.dictConfig(generate_logger_config(args.verbose))

    settings = args.settings

    if args.check:
        sys.exit(check_health(settings))
    else:
        workers = args.workers

        if workers > 1:
            for _ in range(workers - 1):
                p = multiprocessing.Process(target=start, args=(settings,))
                p.start()

        start(
            settings,
            web=args.web,
            extra_web_settings=args.extra_web_settings,
            port=args.port,
        )


if __name__ == "__main__":
    main()
