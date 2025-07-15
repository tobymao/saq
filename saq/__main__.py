import argparse
import os
import sys

from saq.runner import run


def main() -> None:
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
        type=int,
        default=8080,
        help="Web app port, defaults to 8080",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Perform a health check",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Disable automatic logging configuration",
    )

    args = parser.parse_args()

    # Includes the current path as a module path to allow importlib finding in-development modules
    sys.path.append(os.getcwd())

    run(
        args.settings,
        workers=args.workers,
        verbose=args.verbose,
        web=args.web,
        extra_web_settings=args.extra_web_settings,
        port=args.port,
        check=args.check,
        quiet=args.quiet,
    )


if __name__ == "__main__":
    main()
