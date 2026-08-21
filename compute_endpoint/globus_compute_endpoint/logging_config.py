"""
This module contains logging configuration for the globus-compute-endpoint application.
"""

from __future__ import annotations

import logging
import logging.config
import logging.handlers
import os
import pathlib
import re
import sys
from collections import defaultdict
from datetime import datetime

from globus_compute_sdk.sdk.compute_dir import (
    COMPUTE_EP_DIR_ENV,
    ensure_compute_dir,
)

log = logging.getLogger(__name__)

LOG_TS_FMT = "%Y-%m-%d %H:%M:%S,%f"
DEFAULT_FORMAT = (
    "%(asctime)s %(levelname)s %(processName)s-%(process)d "
    "%(threadName)s-%(thread)d %(name)s:%(lineno)d %(funcName)s "
    "%(message)s"
)
LOG_PATH_ENV = "GLOBUS_COMPUTE_LOG_PATH"

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

        kwargs.setdefault("datefmt", LOG_TS_FMT)

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

        line_colors = {
            logging.ERROR: _COL_E,
            logging.WARNING: _COL_W,
            logging.INFO: _COL_I,
            logging.DEBUG: _COL_D,
        }
        self._level_colors = defaultdict(lambda: _COL_D, line_colors)

        w_formatter = DatetimeFormatter(fmt=w_fmt, **kwargs)
        formatters = {
            logging.ERROR: w_formatter,
            logging.WARNING: w_formatter,
            logging.INFO: DatetimeFormatter(fmt="> %(message)s"),
            logging.DEBUG: w_formatter,
        }
        self._formatters = defaultdict(lambda: w_formatter, formatters)
        if debug:
            d_formatter = DatetimeFormatter(fmt=d_fmt, **kwargs)
            formatters = {
                logging.ERROR: DatetimeFormatter(fmt=e_fmt, **kwargs),
                logging.WARNING: DatetimeFormatter(fmt=w_fmt, **kwargs),
                logging.INFO: DatetimeFormatter(fmt=i_fmt, **kwargs),
                logging.DEBUG: d_formatter,
            }
            self._formatters = defaultdict(lambda: d_formatter, formatters)

    def format(self, record: logging.LogRecord):
        ll = self._formatters[record.levelno].format(record)

        if self.use_color:
            # Highlight all UUIDs
            end_coloring = self._level_colors[record.levelno]

            repl = f"{_byel}\\1{_r}{end_coloring}"
            try:
                ll = self.uuid_re.sub(repl, ll)
            except Exception as exc:
                # Basically, inform, but ignore
                print(f"Unable to colorize log message: {exc}")
        return ll


def _get_file_dict_config(
    logpath: pathlib.Path, console_enabled: bool, debug: bool, no_color: bool
) -> dict:
    # ensure that the logdir exists
    logpath.parent.mkdir(parents=True, exist_ok=True)

    # sc-30480: If the log file isn't rotatable (e.g., /dev/stdout)
    # then don't set up the rotation handler
    file_handler = {
        "class": "logging.handlers.RotatingFileHandler",
        "level": "DEBUG",
        "filename": str(logpath),
        "formatter": "filefmt",
        "maxBytes": 100 * 1024 * 1024,
        "backupCount": 1,
    }
    if logpath.exists():
        with open(logpath, "a") as f:
            if not f.seekable():
                file_handler["class"] = "logging.FileHandler"
                file_handler.pop("maxBytes", None)
                file_handler.pop("backupCount", None)

    log_handlers = ["logfile"]

    log_config = {
        "version": 1,
        "formatters": {
            "filefmt": {
                "()": DatetimeFormatter,
                "format": DEFAULT_FORMAT,
                "datefmt": LOG_TS_FMT,
            },
        },
        "handlers": {
            "logfile": file_handler,
        },
        "loggers": {
            "globus_compute_endpoint": {
                "level": "DEBUG" if debug else "INFO",
                "handlers": log_handlers,
            },
            "globus_compute_sdk": {
                "level": "DEBUG" if debug else "WARNING",
                "handlers": log_handlers,
            },
            "parsl": {
                "level": "DEBUG" if debug else "INFO",
                "handlers": log_handlers,
            },
        },
    }

    if console_enabled:
        log_config["formatters"]["streamfmt"] = {  # type: ignore[index]
            "()": ComputeConsoleFormatter,
            "debug": debug,
            "no_color": no_color,
            "datefmt": LOG_TS_FMT,
        }
        log_config["handlers"]["console"] = {  # type: ignore[index]
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "streamfmt",
        }
        log_handlers.append("console")

    return log_config


def _get_stream_dict_config(debug: bool, no_color: bool) -> dict:
    return {
        "version": 1,
        "formatters": {
            "streamfmt": {
                "()": ComputeConsoleFormatter,
                "debug": debug,
                "no_color": no_color,
                "datefmt": LOG_TS_FMT,
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
            "globus_compute_sdk": {
                "level": "DEBUG" if debug else "WARNING",
                "handlers": ["console"],
            },
            "parsl": {
                "level": "DEBUG" if debug else "INFO",
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
        logp = pathlib.Path(logfile)
        config = _get_file_dict_config(logp, console_enabled, debug, no_color)
    else:
        config = _get_stream_dict_config(debug, no_color)

    logging.config.dictConfig(config)


def ensure_paths(ep_name: str | None, custom_paths: dict | None = None) -> pathlib.Path:
    """
    Use jinja2 user config values from paths.endpoint_dir and paths.endpoint_log
    as the UEP directory and UEP log path respectively.

    Ensures that both the directory and the log path are present and valid,
    possibly creating parent directories as well, then returns the Path to the
    log file.

    If no custom config was provided, the endpoint directory is constructed
    from ensure_compute_dir() and ep_name, with the log path default of
    "$GLOBUS_COMPUTE_ENDPOINT_DIR/endpoint.log"

    Note that this method expands user and env vars in the configuration
    strings i.e. ~/abc, $HOME/abc -> /home/some_user/abc and updates the env
    vars in place with the expanded values

    :param ep_name:      The name of the user endpoint.  The default UEP directory
                           is ~/.globus_compute/<ep_name> unless customized
    :param custom_paths: An optional "paths" section from the jinja template
                           where the user can customize the endpoint directory
                           or the log path.  These default to
                           ~/.globus_compute/<ep_name>
                           ~/.globus_compute/<ep_name>/endpoint.log
    """

    # First, get the current values from the environment variables
    ep_dir_str = os.environ.get(COMPUTE_EP_DIR_ENV, "")
    log_path_str = os.environ.get(LOG_PATH_ENV, "")

    # Customized values from jinja user config
    custom_paths = custom_paths or {}
    # Possibly override ENV values with custom configs
    ep_dir_str = custom_paths.get("endpoint_dir") or ep_dir_str
    log_path_str = custom_paths.get("endpoint_log") or log_path_str

    # Use ep_name if nothing else is set
    if not ep_dir_str:
        if not ep_name:
            raise ValueError("Endpoint name must be provided")
        ep_dir_str = str((ensure_compute_dir() / ep_name).resolve())

    ep_dir = pathlib.Path(os.path.expandvars(ep_dir_str)).expanduser()
    if ep_dir.exists() and not ep_dir.is_dir():
        raise ValueError(f"{COMPUTE_EP_DIR_ENV} must be a directory: {ep_dir}")

    # Now update the ep_dir ENV with the final value
    # Parent directory (default -> ~/.globus_compute but could be anywhere)
    # might have already been created but confirm anyway
    ep_dir = ep_dir.resolve()
    ep_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    ep_dir_str = str(ep_dir)
    os.environ[COMPUTE_EP_DIR_ENV] = ep_dir_str

    uep_desc = f" for UEP {ep_name}" if ep_name else ""
    log.info(f"Endpoint directory{uep_desc} set to {ep_dir_str!r}")

    if log_path_str:
        log_path = pathlib.Path(os.path.expandvars(log_path_str)).expanduser()
        if log_path.exists() and log_path.is_dir():
            raise ValueError(f"{LOG_PATH_ENV} can not be a directory: {log_path}")
    else:
        log_path = ep_dir / "endpoint.log"

    # Update the log path ENV with the final value and ensure parent is created
    # Testing of the path's write access is left to the caller in endpoint_manager.py
    log_path = log_path.resolve()
    log_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    log_path_str = str(log_path)

    os.environ[LOG_PATH_ENV] = log_path_str
    log.info(f"{LOG_PATH_ENV} has been set to {log_path_str!r}")

    return log_path
