"""
This module contains logging configuration for the funcx-endpoint application.
"""

import logging
import logging.config
import logging.handlers
import pathlib
import typing as t

log = logging.getLogger(__name__)

_DEFAULT_LOGFILE = str(pathlib.Path.home() / ".funcx" / "endpoint.log")


def setup_logging(
    *,
    logfile: t.Optional[str] = None,
    console_enabled: bool = True,
    debug: bool = False
) -> None:
    if logfile is None:
        logfile = _DEFAULT_LOGFILE

    default_config = {
        "version": 1,
        "formatters": {
            "streamfmt": {
                "format": "%(asctime)s %(name)s:%(lineno)d [%(levelname)s] %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "filefmt": {
                "format": (
                    "%(asctime)s.%(msecs)03d "
                    "%(name)s:%(lineno)d [%(levelname)s] %(message)s"
                ),
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
                "formatter": "streamfmt",
            },
            "logfile": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "DEBUG",
                "filename": logfile,
                "formatter": "filefmt",
                "maxBytes": 100 * 1024 * 1024,
                "backupCount": 1,
            },
        },
        "loggers": {
            "funcx_endpoint": {
                "level": "DEBUG" if debug else "INFO",
                "handlers": ["console", "logfile"] if console_enabled else ["logfile"],
            },
            # configure for the funcx SDK as well
            "funcx": {
                "level": "DEBUG" if debug else "WARNING",
                "handlers": ["logfile", "console"] if console_enabled else ["logfile"],
            },
        },
    }

    logging.config.dictConfig(default_config)

    if debug:
        log.debug("debug logging enabled")
