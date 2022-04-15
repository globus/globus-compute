import typing as t
from concurrent.futures import Future


class FuncXFuture(Future):
    """Extends concurrent.futures.Future to include an optional task UUID."""

    task_id: t.Optional[str]
    """The UUID for the task behind this Future. In batch mode, this will
    not be populated immediately, but will appear later when the task is
    submitted to the FuncX services."""

    def __init__(self, task_id: t.Optional[str] = None):
        super().__init__()
        self.task_id = task_id
