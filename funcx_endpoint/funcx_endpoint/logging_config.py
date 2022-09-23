"""
This module contains logging configuration for the funcx-endpoint application.
"""

import logging
import logging.config
import logging.handlers
import os
import re
import sys
import typing as t

log = logging.getLogger(__name__)

DEFAULT_FORMAT = (
    "%(created)f %(asctime)s %(levelname)s %(processName)s-%(process)d "
    "%(threadName)s-%(thread)d %(name)s:%(lineno)d %(funcName)s "
    "%(message)s"
)

_ital = "\033[3m"
_redb = "\033[41m"
_teal = "\033[32m"
_yel = "\033[33m"
_byel = "\033[93m"
_yelb = "\033[43m"
_purp = "\033[35m"
_cyan = "\033[36m"
_gray = "\033[37m"
_grayonb = "\033[37;40m"
_r = "\033[m"
_C_BASE = (
    f"{_teal}%(created)f{_r} {_yel}%(asctime)s{_r} {_ital}%(levelname)s{_r}"
    " %(processName)s-%(process)d %(threadName)s-%(thread)d"
    f" %(name)s:{_cyan}%(lineno)d{_r} {_purp}%(funcName)s{_r}"
)
COLOR_ERROR = _redb
COLOR_WARNING = _yelb
COLOR_INFO = _gray
COLOR_DEBUG = _grayonb
C_ERROR_FMT = _C_BASE + f" {COLOR_ERROR}%(message)s{_r}"
C_WARNING_FMT = _C_BASE + f" {COLOR_WARNING}%(message)s{_r}"
C_INFO_FMT = _C_BASE + f" {COLOR_INFO}%(message)s{_r}"
C_DEBUG_FMT = _C_BASE + f" {COLOR_DEBUG}%(message)s{_r}"


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

    _u = "[0-9A-Fa-f]"  # convenience
    _uuid_re = f"{_u}{{8}}-{_u}{{4}}-{_u}{{4}}-{_u}{{4}}-{_u}{{12}}"
    # match uuids for colorization that have not otherwise already been colorized
    uuid_re = re.compile(rf"(?<!\dm)({_uuid_re})")

    def __init__(
        self,
        debug: bool = False,
        no_color: bool = False,
        fmt: str = "",
        datefmt: str = "%Y-%m-%d %H:%M:%S",
    ) -> None:
        super().__init__()

        self.use_color = debug and not no_color and sys.stderr.isatty()

        if fmt:
            d_fmt, i_fmt, w_fmt, e_fmt = fmt, fmt, fmt, fmt
        else:
            d_fmt, i_fmt, w_fmt, e_fmt = (
                C_DEBUG_FMT,
                C_INFO_FMT,
                C_WARNING_FMT,
                C_ERROR_FMT,
            )

        if not self.use_color:
            ansi_re = re.compile("\033.*?m")
            d_fmt = ansi_re.sub("", d_fmt)
            i_fmt = ansi_re.sub("", i_fmt)
            w_fmt = ansi_re.sub("", w_fmt)
            e_fmt = ansi_re.sub("", e_fmt)

        if debug:
            self._error_formatter = logging.Formatter(fmt=e_fmt, datefmt=datefmt)
            self._warning_formatter = logging.Formatter(fmt=w_fmt, datefmt=datefmt)
            self._debug_formatter = logging.Formatter(fmt=d_fmt, datefmt=datefmt)
            self._info_formatter = logging.Formatter(fmt=i_fmt, datefmt=datefmt)
        else:
            self._info_formatter = logging.Formatter(fmt="> %(message)s")
            self._warning_formatter = logging.Formatter(fmt=w_fmt, datefmt=datefmt)
            self._error_formatter = self._warning_formatter
            self._debug_formatter = self._warning_formatter

    def format(self, record: logging.LogRecord):
        if self.use_color:
            # Highlight all UUIDs
            if record.levelno > logging.WARNING:
                end_coloring = COLOR_ERROR
            elif record.levelno > logging.INFO:
                end_coloring = COLOR_WARNING
            elif record.levelno > logging.DEBUG:
                end_coloring = COLOR_INFO
            else:
                end_coloring = COLOR_DEBUG

            repl = f"{_byel}\\1{_r}{end_coloring}"
            try:
                record.msg = self.uuid_re.sub(repl, record.msg)
                if isinstance(record.args, dict):
                    for k, v in record.args.items():
                        record.args[k] = self.uuid_re.sub(repl, str(v))
                elif record.args:
                    args = tuple(self.uuid_re.sub(repl, str(a)) for a in record.args)
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
                "()": "funcx_endpoint.logging_config.FuncxConsoleFormatter",
                "debug": debug,
                "no_color": no_color,
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
                "handlers": log_handlers,
            },
            # configure for the funcx SDK as well
            "funcx": {
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
                "()": "funcx_endpoint.logging_config.FuncxConsoleFormatter",
                "debug": debug,
                "no_color": no_color,
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
    debug: bool = False,
    no_color: bool = False,
) -> None:
    add_trace_level()

    if logfile is not None:
        config = _get_file_dict_config(logfile, console_enabled, debug, no_color)
    else:
        config = _get_stream_dict_config(debug, no_color)

    logging.config.dictConfig(config)
