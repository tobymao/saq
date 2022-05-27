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
        "formatters": {
            "saq_formatter": {
                "format": "%(levelname)s:%(name)s:%(message)s",
            },
        },
        "handlers": {
            "saq_handler": {
                "level": log_level,
                "class": "logging.StreamHandler",
                "formatter": "saq_formatter",
            },
        },
        "loggers": {
            "saq": {
                "level": log_level,
                "handlers": ["saq_handler"],
            },
        },
    }
