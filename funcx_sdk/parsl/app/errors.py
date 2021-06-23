import dill
from six import reraise
from tblib import Traceback
from functools import wraps
from typing import Callable, Union, Any, TypeVar
from types import TracebackType
import logging
logger = logging.getLogger(__name__)


class RemoteExceptionWrapper:
    def __init__(self, e_type: type, e_value: Exception, traceback: TracebackType) -> None:

        self.e_type = dill.dumps(e_type)
        self.e_value = dill.dumps(e_value)
        self.e_traceback = Traceback(traceback)

    def reraise(self) -> None:

        t = dill.loads(self.e_type)

        # the type is logged here before deserialising v and tb
        # because occasionally there are problems deserialising the
        # value (see #785, #548) and the fix is related to the
        # specific exception type.
        logger.debug("Reraising exception of type {}".format(t))

        v = dill.loads(self.e_value)
        tb = self.e_traceback.as_traceback()

        reraise(t, v, tb)


R = TypeVar('R')


def wrap_error(func: Callable[..., R]) -> Callable[..., Union[R, RemoteExceptionWrapper]]:
    @wraps(func)  # type: ignore
    def wrapper(*args: object, **kwargs: object) -> Any:
        import sys
        from funcx.serialize.errors import RemoteExceptionWrapper
        try:
            return func(*args, **kwargs)  # type: ignore
        except Exception:
            return RemoteExceptionWrapper(*sys.exc_info())
    return wrapper  # type: ignore
