"""
This module provides helpers for constructing the internal dict objects which get passed
around and ultimately converted into funcx_common.messagepack.Result objects

IDEALLY this would be refactored to produce and return Result objects directly, which
could then be passed around internally instead of raw dicts
but we don't have time to do that (even though it would be better (a lot better))
"""

from __future__ import annotations

import sys
import traceback
import types
import typing as t

from funcx.errors import MaxResultSizeExceeded
from funcx_endpoint.exceptions import CouldNotExecuteUserTaskError

INTERNAL_ERROR_CLASSES: tuple[type[Exception], ...] = (
    CouldNotExecuteUserTaskError,
    MaxResultSizeExceeded,
)


def _typed_excinfo() -> tuple[type[Exception], Exception, types.TracebackType]:
    return t.cast(
        t.Tuple[t.Type[Exception], Exception, types.TracebackType],
        sys.exc_info(),
    )


def _inner_traceback(tb: types.TracebackType, levels: int = 2) -> types.TracebackType:
    while levels > 0:
        tb = tb.tb_next if tb.tb_next is not None else tb
        levels -= 1
    return tb


def get_error_string(*, tb_levels: int = 2) -> str:
    exc_info = _typed_excinfo()
    exc_type, exc, tb = exc_info
    if isinstance(exc, INTERNAL_ERROR_CLASSES):
        return repr(exc)
    return "".join(
        traceback.format_exception(
            exc_type, exc, _inner_traceback(tb, levels=tb_levels)
        )
    )


def get_result_error_details() -> tuple[str, str]:
    _, error, _ = _typed_excinfo()
    # code, user_message
    if isinstance(error, INTERNAL_ERROR_CLASSES):
        return (error.__class__.__name__, f"remote error: {error}")
    return (
        "RemoteExecutionError",
        "An error occurred during the execution of this task",
    )
