from __future__ import annotations

import textwrap
import time

from globus_compute_sdk.sdk.utils import get_env_details


def check_version(task_details: dict | None) -> str:
    if task_details:
        worker_py = task_details.get("python_version", "UnknownPy")
        worker_os = task_details.get("os", "UnknownOS")
        sdk_py = get_env_details()["python_version"]
        if sdk_py != worker_py:
            return (
                f"Warning:  Client uses Python {sdk_py} "
                f"but worker used {worker_py} on {worker_os}. "
            )
        else:
            return f"Task execution info: {task_details}"

    # Add no text to existing error msg if no info
    return ""


class ComputeError(Exception):
    """Base class for all funcx exceptions"""

    def __str__(self):
        return self.__repr__()


class VersionMismatch(ComputeError):
    """Either client and endpoint version mismatch, or version cannot be retrieved."""

    def __init__(self, version_message):
        self.version_message = version_message

    def __repr__(self):
        return f"Globus Compute Versioning Issue: {self.version_message}"


class SerializationError(ComputeError):
    """Something failed during serialization or deserialization."""

    def __init__(self, message, task_details: dict | None = None):
        self.message = message
        self.task_details = task_details

    def __repr__(self):
        return (
            check_version(self.task_details)
            + "Serialization Error during: {self.message}"
        )


class TaskPending(ComputeError):
    """Task is pending and no result is available yet"""

    def __init__(self, reason):
        self.reason = reason

    def __repr__(self):
        return f"Task is pending due to {self.reason}"


class MaxResultSizeExceeded(Exception):
    """Result produced by the function exceeds the maximum supported result size
    threshold"""

    def __init__(self, result_size: int, result_size_limit: int):
        self.result_size = result_size
        self.result_size_limit = result_size_limit

    def __str__(self) -> str:
        return (
            f"Task result of {self.result_size}B exceeded current "
            f"limit of {self.result_size_limit}B"
        )


class TaskExecutionFailed(Exception):
    """
    Error result from the remote end, wrapped as an exception object
    """

    def __init__(
        self,
        remote_data: str,
        completion_t: str | None = None,
        task_details: dict | None = None,
    ):
        self.remote_data = remote_data
        self.task_details = task_details
        # Fill in completion time if missing
        self.completion_t = completion_t or str(time.time())

    def __str__(self) -> str:
        return (
            check_version(self.task_details)
            + "\n"
            + textwrap.indent(self.remote_data, "    ")
        )
