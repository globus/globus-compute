import logging
from functools import wraps
from types import TracebackType
from typing import Any, Callable, TypeVar, Union

import dill
from tblib import Traceback

logger = logging.getLogger(__name__)


class RemoteExceptionWrapper:
    def __init__(
        self, e_type: type, e_value: Exception, traceback: TracebackType
    ) -> None:
        self.e_type = dill.dumps(e_type)
        self.e_value = dill.dumps(e_value)
        self.e_traceback = Traceback(traceback)

    def reraise(self) -> None:
        t = dill.loads(self.e_type)

        # the type is logged here before deserialising v and tb
        # because occasionally there are problems deserialising the
        # value (see #785, #548) and the fix is related to the
        # specific exception type.
        logger.debug(f"Reraising exception of type {t}")

        v = dill.loads(self.e_value)
        tb = self.e_traceback.as_traceback()

        raise v.with_traceback(tb)


R = TypeVar("R")


def wrap_error(
    func: Callable[..., R]
) -> Callable[..., Union[R, RemoteExceptionWrapper]]:
    @wraps(func)  # type: ignore
    def wrapper(*args: object, **kwargs: object) -> Any:
        import sys

        from globus_compute_sdk.serialize.errors import RemoteExceptionWrapper

        try:
            return func(*args, **kwargs)  # type: ignore
        except Exception:
            return RemoteExceptionWrapper(*sys.exc_info())

    return wrapper  # type: ignore
