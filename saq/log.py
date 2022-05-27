import logging


def generate_logger_config(verbosity):
    if verbosity == 0:
        log_level = logging.ERROR
    elif verbosity == 1:
        log_level = logging.INFO
    else:
        log_level = logging.DEBUG

    return {
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {
            "standard": {
                "level": log_level,
                "class": "logging.StreamHandler",
            },
        },
        "loggers": {
            "saq": {
                "handlers": ["standard"],
                "level": log_level,
            },
        },
    }
