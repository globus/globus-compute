from __future__ import annotations

import os
import sys
import typing as t
import warnings
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from platform import platform

import dill
import globus_compute_sdk.version as sdk_version
import packaging.version
from packaging.version import Version


def chunk_by(iterable: t.Iterable, size) -> t.Iterable[tuple]:
    to_chunk_iter = iter(iterable)
    return iter(lambda: tuple(islice(to_chunk_iter, size)), ())


def _log_tmp_file(item, filename: Path | None = None) -> None:
    """
    Convenience method for logging to a tmp file for debugging, getting
    around most thread or logging setup complexities.
    Usage of this should be removed after dev testing is done

    Not intended for non-development debugging usage

    :param item: The info to log to file
    :param filename: An optional path to the log file.   If None,
                      use a file in the current user's home directory
    """
    if filename is None:
        filename = Path.home() / "compute_debug.log"
    with open(filename, "a") as f:
        ts = datetime.now(timezone.utc).astimezone().isoformat()
        # Console as well as write to file
        print(ts, item, file=f)


def get_py_version_str() -> str:
    return "{}.{}.{}".format(*sys.version_info)


def get_env_details() -> t.Dict[str, t.Any]:
    return {
        "os": platform(),
        "dill_version": dill.__version__,
        "python_version": get_py_version_str(),
        "globus_compute_sdk_version": sdk_version.__version__,
    }


def check_version(task_details: dict | None, check_py_micro: bool = True) -> str | None:
    """
    This method adds an optional string to some Exceptions below to
    warn of differing environments as a possible cause.  It is prepended

    :param task_details:    Task details from worker environment
    :param check_py_minor:  Whether Python micro version mismatches should
                              trigger a warning, or only major/minor diffs
    """
    if task_details:
        worker_py = task_details.get("python_version", "UnknownPy")
        worker_dill = task_details.get("dill_version", "UnknownDill")
        worker_epid = task_details.get("endpoint_id", "<unknown>")
        client_details = get_env_details()

        sdk_py = client_details["python_version"]
        python_mismatch = True
        if worker_py != "UnknownPy":
            try:
                sdk_v = Version(sdk_py)
                worker_v = Version(worker_py)
                python_mismatch = (sdk_v.major, sdk_v.minor) != (
                    worker_v.major,
                    worker_v.minor,
                )
                if python_mismatch is False and check_py_micro:
                    python_mismatch = sdk_v.micro != worker_v.micro
            except packaging.version.InvalidVersion:
                # Python version invalid mostly from mocks, handle as mismatch
                pass

        sdk_dill = client_details["dill_version"]
        if python_mismatch or sdk_dill != worker_dill:
            return (
                "\nEnvironment differences detected between local SDK and "
                f"endpoint {worker_epid} workers:\n"
                f"\t    SDK: Python {sdk_py}/Dill {sdk_dill}\n"
                f"\tWorkers: Python {worker_py}/Dill {worker_dill}\n"
                f"This may cause serialization issues.  See "
                "https://globus-compute.readthedocs.io/en/latest/sdk.html#avoiding-serialization-errors "  # noqa"
                "for more information."
            )
    return None


@t.overload
def get_env_var_with_deprecation(
    current_name: str, old_name: str, default: None = None
) -> str | None: ...


@t.overload
def get_env_var_with_deprecation(
    current_name: str, old_name: str, default: str
) -> str: ...


def get_env_var_with_deprecation(
    current_name: str, old_name: str, default: str | None = None
) -> str | None:
    if old_name in os.environ:
        warnings.warn(
            f"{old_name} is deprecated and will be removed in a future release. "
            f"Use {current_name} instead."
        )

    return os.getenv(current_name, os.getenv(old_name, default))


def display_name(name: str):
    """
    Used for descriptions of commands
    :param name:  A short string describing the method, currently used when
                    displaying headers when running diagnostic tests.
    """

    def decorator(func):
        func.display_name = name
        return func

    return decorator
