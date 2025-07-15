from __future__ import annotations

import logging
import multiprocessing
import sys

from saq.worker import check_health, start


def run(
    settings: str,
    /,
    *,
    workers: int = 1,
    verbose: int = 0,
    web: bool = False,
    extra_web_settings: list[str] | None = None,
    port: int = 8080,
    check: bool = False,
    quiet: bool = False,
) -> None:
    if not quiet:
        level = verbose

        if level == 0:
            level = logging.WARNING
        elif level == 1:
            level = logging.INFO
        else:
            level = logging.DEBUG

        logging.basicConfig(level=level)

    if check:
        sys.exit(check_health(settings))
    else:
        if workers > 1:
            for _ in range(workers - 1):
                p = multiprocessing.Process(target=start, args=(settings,))
                p.start()

        try:
            start(
                settings,
                web=web,
                extra_web_settings=extra_web_settings,
                port=port,
            )
        except KeyboardInterrupt:
            pass
