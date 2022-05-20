"""
This module contains logging configuration for the funcx-endpoint application.
"""

import logging
import logging.config
import logging.handlers
import os
import typing as t

log = logging.getLogger(__name__)

DEFAULT_FORMAT = (
    "%(created)f %(asctime)s %(levelname)s %(processName)s-%(process)d "
    "%(threadName)s-%(thread)d %(name)s:%(lineno)d %(funcName)s "
    "%(message)s"
)


class FuncxConsoleFormatter(logging.Formatter):
    """
    For internal use only.
    This formatter handles output to standard streams in the following way:

    if 'debug' is False (default):
        info messages and below are treated as "user output" and are minimally decorated
        warning messages and up are given a full format, so are debug messages

    if 'debug' is True:
        all messages are given the full format
    """

    def __init__(
        self,
        debug: bool = False,
        fmt: str = DEFAULT_FORMAT,
        datefmt: str = "%Y-%m-%d %H:%M:%S",
    ) -> None:
        super().__init__()

        self._warning_formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
        if debug:
            self._info_formatter = self._warning_formatter
        else:
            self._info_formatter = logging.Formatter(fmt="> %(message)s")

    def format(self, record):
        if record.levelno > logging.INFO:
            return self._warning_formatter.format(record)
        return self._info_formatter.format(record)


def _get_file_dict_config(logfile: str, console_enabled: bool, debug: bool) -> dict:
    # ensure that the logdir exists
    logdir = os.path.dirname(logfile)
    os.makedirs(logdir, exist_ok=True)

    return {
        "version": 1,
        "formatters": {
            "streamfmt": {
                "()": "funcx_endpoint.logging_config.FuncxConsoleFormatter",
                "debug": debug,
            },
            "filefmt": {
                "format": DEFAULT_FORMAT,
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


def _get_stream_dict_config(debug: bool) -> dict:
    return {
        "version": 1,
        "formatters": {
            "streamfmt": {
                "()": "funcx_endpoint.logging_config.FuncxConsoleFormatter",
                "debug": debug,
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "DEBUG" if debug else "INFO",
                "formatter": "streamfmt",
            }
        },
        "loggers": {
            "funcx_endpoint": {
                "level": "DEBUG",
                "handlers": ["console"],
            },
            # configure for the funcx SDK as well
            "funcx": {
                "level": "DEBUG" if debug else "WARNING",
                "handlers": ["console"],
            },
        },
    }


def add_trace_level() -> None:
    """This adds a trace level to the logging system.

    See https://stackoverflow.com/questions/2183233
    """

    TRACE = 5
    logging.TRACE = TRACE  # type: ignore[attr-defined]

    def logForLevel(self, message, *args, **kwargs):
        if self.isEnabledFor(TRACE):
            self._log(TRACE, message, args, **kwargs)

    def logToRoot(message, *args, **kwargs):
        logging.log(TRACE, message, *args, **kwargs)

    logging.addLevelName(TRACE, "TRACE")
    logging.getLoggerClass().trace = logForLevel  # type: ignore[attr-defined]
    logging.trace = logToRoot  # type: ignore[attr-defined]


def setup_logging(
    *,
    logfile: t.Optional[str] = None,
    console_enabled: bool = True,
    debug: bool = False
) -> None:

    add_trace_level()

    if logfile is not None:
        config = _get_file_dict_config(logfile, console_enabled, debug)
    else:
        config = _get_stream_dict_config(debug)

    logging.config.dictConfig(config)
    if debug:
        log.debug("debug logging enabled")
