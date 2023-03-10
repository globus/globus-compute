from __future__ import annotations

import textwrap
import time


class FuncxError(Exception):
    """Base class for all funcx exceptions"""

    def __str__(self):
        return self.__repr__()


class VersionMismatch(FuncxError):
    """Either client and endpoint version mismatch, or version cannot be retrieved."""

    def __init__(self, version_message):
        self.version_message = version_message

    def __repr__(self):
        return f"FuncX Versioning Issue: {self.version_message}"


class SerializationError(FuncxError):
    """Something failed during serialization or deserialization."""

    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return f"Serialization Error during: {self.message}"


class TaskPending(FuncxError):
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


class FuncxTaskExecutionFailed(Exception):
    """
    Error result from the remote end, wrapped as an exception object
    """

    def __init__(self, remote_data: str, completion_t: str | None = None):
        self.remote_data = remote_data
        # Fill in completion time if missing
        self.completion_t = completion_t or str(time.time())

    def __str__(self) -> str:
        return "\n" + textwrap.indent(self.remote_data, "    ")
