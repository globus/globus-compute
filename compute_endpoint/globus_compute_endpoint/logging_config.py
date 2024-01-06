"""
This module contains logging configuration for the globus-compute-endpoint application.
"""
from __future__ import annotations

import copy
import logging
import logging.config
import logging.handlers
import os
import pathlib
import re
import sys
import uuid
from datetime import datetime

log = logging.getLogger(__name__)

DEFAULT_FORMAT = (
    "%(asctime)s %(levelname)s %(processName)s-%(process)d "
    "%(threadName)s-%(thread)d %(name)s:%(lineno)d %(funcName)s "
    "%(message)s"
)

_und = "\033[4m"
_ital = "\033[3m"
_green = "\033[32m"
_bblack = "\033[1;30m"
_byel = "\033[93m"
_purp = "\033[35m"
_cyan = "\033[36m"
_gray = "\033[37m"
_bredonb = "\033[91;40m"
_yelonb = "\033[33;40m"
_grayonb = "\033[2;37;40m"
_r = "\033[m"
_C_FMT = (
    f"{_und}%(asctime)s{_r} {_ital}{{LEVEL_COLOR}}%(levelname)s{_r}"
    f" %(processName)s-%(process)d {_green}%(threadName)s-%(thread)d{_r}"
    f" {_bblack}%(name)s{_r}:{_cyan}%(lineno)d{_r} {_purp}%(funcName)s{_r}"
    f" {{LEVEL_COLOR}}%(message)s{_r}"
)
_COL_E = _bredonb
_COL_W = _yelonb
_COL_D = _grayonb
_COL_I = _gray
C_E_FMT = _C_FMT.format(LEVEL_COLOR=_COL_E)
C_W_FMT = _C_FMT.format(LEVEL_COLOR=_COL_W)
C_I_FMT = _C_FMT.format(LEVEL_COLOR=_COL_I)
C_D_FMT = _C_FMT.format(LEVEL_COLOR=_COL_D)


class DatetimeFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ct = datetime.fromtimestamp(record.created)
        if not datefmt:
            datefmt = self.default_time_format
        return ct.strftime(datefmt)


class ComputeConsoleFormatter(logging.Formatter):
    """
    For internal use only.
    This formatter handles output to standard streams in the following way:

    if 'debug' is False (default):
        info messages and below are treated as "user output" and are minimally decorated
        warning messages and up are given a full format, so are debug messages

    if 'debug' is True:
        all messages are given the full format
    """

    _u = "[0-9A-Fa-f]"  # convenience
    _uuid_re = f"{_u}{{8}}-{_u}{{4}}-{_u}{{4}}-{_u}{{4}}-{_u}{{12}}"
    # match uuids for colorization that have not otherwise already been colorized
    uuid_re = re.compile(rf"(?<!\dm)({_uuid_re})")

    def __init__(
        self, debug: bool = False, no_color: bool = False, fmt: str = "", **kwargs
    ) -> None:
        super().__init__()

        kwargs.setdefault("datefmt", "%Y-%m-%d %H:%M:%S,%f")

        self.use_color = debug and not no_color and sys.stderr.isatty()

        if fmt:
            d_fmt, i_fmt, w_fmt, e_fmt = fmt, fmt, fmt, fmt
        else:
            d_fmt, i_fmt, w_fmt, e_fmt = C_D_FMT, C_I_FMT, C_W_FMT, C_E_FMT

        if not self.use_color:
            ansi_re = re.compile("\033.*?m")
            d_fmt = ansi_re.sub("", d_fmt)
            i_fmt = ansi_re.sub("", i_fmt)
            w_fmt = ansi_re.sub("", w_fmt)
            e_fmt = ansi_re.sub("", e_fmt)

        if debug:
            self._error_formatter = DatetimeFormatter(fmt=e_fmt, **kwargs)
            self._warning_formatter = DatetimeFormatter(fmt=w_fmt, **kwargs)
            self._debug_formatter = DatetimeFormatter(fmt=d_fmt, **kwargs)
            self._info_formatter = DatetimeFormatter(fmt=i_fmt, **kwargs)
        else:
            self._info_formatter = DatetimeFormatter(fmt="> %(message)s")
            self._warning_formatter = DatetimeFormatter(fmt=w_fmt, **kwargs)
            self._error_formatter = self._warning_formatter
            self._debug_formatter = self._warning_formatter

    def format(self, record: logging.LogRecord):
        if self.use_color:
            # Highlight all UUIDs
            if record.levelno > logging.WARNING:
                end_coloring = _COL_E
            elif record.levelno > logging.INFO:
                end_coloring = _COL_W
            elif record.levelno > logging.DEBUG:
                end_coloring = _COL_I
            else:
                end_coloring = _COL_D

            repl = f"{_byel}\\1{_r}{end_coloring}"
            try:
                record.msg = self.uuid_re.sub(repl, record.msg)
                if isinstance(record.args, dict):
                    record.args = copy.deepcopy(record.args)
                    for k, v in record.args.items():
                        if isinstance(v, (str, uuid.UUID)):
                            record.args[k] = self.uuid_re.sub(repl, str(v))
                elif record.args:
                    uu_sub = self.uuid_re.sub
                    args = tuple(
                        uu_sub(repl, str(a)) if isinstance(a, (str, uuid.UUID)) else a
                        for a in record.args
                    )
                    record.args = args
            except Exception as exc:
                # Basically, inform, but ignore
                print(f"Unable to colorize log message: {exc}")

        if record.levelno > logging.WARNING:
            return self._error_formatter.format(record)
        elif record.levelno > logging.INFO:
            return self._warning_formatter.format(record)
        elif record.levelno > logging.DEBUG:
            return self._info_formatter.format(record)
        return self._debug_formatter.format(record)


def _get_file_dict_config(
    logfile: str, console_enabled: bool, debug: bool, no_color: bool
) -> dict:
    # ensure that the logdir exists
    logdir = os.path.dirname(logfile)
    os.makedirs(logdir, exist_ok=True)
    log_handlers = ["logfile"]
    if console_enabled:
        log_handlers.append("console")

    return {
        "version": 1,
        "formatters": {
            "streamfmt": {
                "()": ComputeConsoleFormatter,
                "debug": debug,
                "no_color": no_color,
                "datefmt": "%Y-%m-%d %H:%M:%S,%f",
            },
            "filefmt": {
                "()": DatetimeFormatter,
                "format": DEFAULT_FORMAT,
                "datefmt": "%Y-%m-%d %H:%M:%S,%f",
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
            "globus_compute_endpoint": {
                "level": "DEBUG" if debug else "INFO",
                "handlers": log_handlers,
            },
            # configure for the Globus Compute SDK as well
            "globus_compute_sdk": {
                "level": "DEBUG" if debug else "WARNING",
                "handlers": log_handlers,
            },
        },
    }


def _get_stream_dict_config(debug: bool, no_color: bool) -> dict:
    return {
        "version": 1,
        "formatters": {
            "streamfmt": {
                "()": ComputeConsoleFormatter,
                "debug": debug,
                "no_color": no_color,
                "datefmt": "%Y-%m-%d %H:%M:%S,%f",
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
            "globus_compute_endpoint": {
                "level": "DEBUG",
                "handlers": ["console"],
            },
            # configure for the Globus Compute SDK as well
            "globus_compute_sdk": {
                "level": "DEBUG" if debug else "WARNING",
                "handlers": ["console"],
            },
        },
    }


class ComputeLogger(logging.Logger):
    TRACE = logging.DEBUG - 5

    def trace(self, msg, *args, **kwargs):
        self.log(ComputeLogger.TRACE, msg, args, **kwargs)


logging.setLoggerClass(ComputeLogger)
logger = logging.getLogger(__name__)


def setup_logging(
    *,
    logfile: pathlib.Path | str | None = None,
    console_enabled: bool = True,
    debug: bool = False,
    no_color: bool = False,
) -> None:
    if logfile is not None:
        config = _get_file_dict_config(str(logfile), console_enabled, debug, no_color)
    else:
        config = _get_stream_dict_config(debug, no_color)

    logging.config.dictConfig(config)
