from __future__ import annotations

import functools
import itertools
import logging
import os
import threading
import time
import typing as t
import uuid
from abc import ABC, abstractmethod
from concurrent.futures import Future

from globus_compute_common import messagepack
from globus_compute_common.messagepack.message_types import (
    EPStatusReport,
    Result,
    TaskTransition,
)
from globus_compute_common.tasks import ActorName, TaskState
from globus_compute_endpoint.engines.helper import execute_task
from globus_compute_endpoint.exception_handling import (
    get_error_string,
    get_result_error_details,
)
from globus_compute_sdk.sdk.utils.uuid_like import (
    UUID_LIKE_T,
    as_optional_uuid,
    as_uuid,
)
from globus_compute_sdk.serialize.facade import ComputeSerializer, DeserializerAllowlist
from parsl.utils import RepresentationMixin

logger = logging.getLogger(__name__)

_EXC_HISTORY_TMPL = "+" * 68 + "\nTraceback from attempt: {ndx}\n{exc}" + "-" * 68
_EXC_NO_HISTORY_TMPL = "+" * 68 + "\n{exc}" + "-" * 68


class GCFuture(Future):
    __slots__ = (
        "_gc_task_id",
        "_ex_task_id",
        "_block_id",
        "_job_id",
        "_function_id",
        "_observers",
    )

    def __init__(
        self,
        gc_task_id: UUID_LIKE_T,
        *,
        function_id: UUID_LIKE_T | None = None,
        block_id: t.Any = None,
        executor_task_id: t.Any = None,
        job_id: t.Any = None,
    ):
        super().__init__()
        self._observers: dict[str, list[t.Callable[[GCFuture, t.Any], None]]] = {
            "block_id": [],
            "executor_task_id": [],
            "job_id": [],
        }
        self._block_id = block_id
        self._ex_task_id = executor_task_id
        self._job_id = job_id

        _tmp = function_id  # work with both mypy and flake8
        self.function_id = _tmp  # type: ignore[assignment]

        self.gc_task_id = gc_task_id

    def __repr__(self) -> str:
        cn = type(self).__name__
        gc_task_id = str(self.gc_task_id)
        parts = [f"{gc_task_id!r}"]
        fid = self.function_id
        executor_task_id = self.executor_task_id
        block_id = self.block_id
        job_id = self.job_id
        if fid is not None:
            function_id = str(fid)
            parts.append(f"{function_id=!r}")
        if executor_task_id is not None:
            parts.append(f"{executor_task_id=!r}")
        if block_id:
            parts.append(f"{block_id=!r}")
        if job_id:
            parts.append(f"{job_id=!r}")
        return f"{cn}({', '.join(parts)})"

    def bind(self, var_name: str, cb: t.Callable) -> None:
        self._observers[var_name].append(cb)

    @property
    def gc_task_id(self):
        return self._gc_task_id

    @gc_task_id.setter
    def gc_task_id(self, val: UUID_LIKE_T):
        self._gc_task_id = as_uuid(val)

    @property
    def function_id(self) -> uuid.UUID | None:
        return self._function_id

    @function_id.setter
    def function_id(self, val: UUID_LIKE_T | None):
        self._function_id = as_optional_uuid(val)

    @property
    def block_id(self):
        return self._block_id

    @block_id.setter
    def block_id(self, val):
        if val != self._block_id:
            self._block_id = val
            for cb in self._observers["block_id"]:
                cb(self, val)

    @property
    def executor_task_id(self):
        return self._ex_task_id

    @executor_task_id.setter
    def executor_task_id(self, val):
        if val != self._ex_task_id:
            self._ex_task_id = val
            for cb in self._observers["executor_task_id"]:
                cb(self, val)

    @property
    def job_id(self):
        return self._job_id

    @job_id.setter
    def job_id(self, val):
        if val != self._job_id:
            self._job_id = val
            for cb in self._observers["job_id"]:
                cb(self, val)


class GCExecutorFuture(Future):
    __slots__ = ("executor_task_id",)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.executor_task_id: t.Any = None


class GlobusComputeEngineBase(ABC, RepresentationMixin):
    """Shared functionality and interfaces required by all GlobusCompute Engines.
    This is designed to plug-in executors following the concurrent.futures.Executor
    interface as execution backends to GlobusCompute
    """

    def __init__(
        self,
        *args: object,
        endpoint_id: uuid.UUID | None = None,
        max_retries_on_system_failure: int = 0,
        working_dir: str | os.PathLike = "tasks_working_dir",
        allowed_serializers: DeserializerAllowlist | None = None,
        **kwargs: object,
    ):
        """
        Parameters
        ----------

        endpoint_id: uuid | None
            ID of the endpoint that the engine serves as execution backend

        max_retries_on_system_failure: int
            Set the number of retries for functions that fail due to system
            failures such as node failure/loss. Since functions can fail
            after partial runs, consider additional cleanup logic before
            enabling this functionality. default=0

        working_dir: str | os.PathLike
            Directory within which functions should execute, defaults to
            (~/.globus_compute/<endpoint_name>/tasks_working_dir)
            If a relative path is supplied, the working dir is set relative
            to the endpoint.run_dir. If an absolute path is supplied, it is
            used as is. default="tasks_working_dir"

        allowed_serializers: DeserializerAllowlist | None
            A list of serialization strategy types or import paths to such
            types, which the engine's serializer will check against whenever
            deserializing user submissions. If falsy, every serializer is
            allowed. See ComputeSerializer for more details. default=None

        kwargs
        """
        self._shutdown_event = threading.Event()
        self.endpoint_id = endpoint_id
        self.serde = ComputeSerializer(allowed_deserializer_types=allowed_serializers)
        self.max_retries_on_system_failure = max_retries_on_system_failure
        # remove these unused vars that we are adding to just keep
        # endpoint interchange happy
        self.container_type: str | None = None
        self.run_dir: str | None = None
        self.working_dir: str | os.PathLike = working_dir
        self.run_in_sandbox: bool = False

        self._task_id_map: dict[t.Any, GCFuture] = {}

        # This attribute could be set by the subclasses in their
        # start method if another component insists on owning the queue.
        self._engine_ready: bool = False

    @abstractmethod
    def assert_ha_compliant(self):
        """
        Signal to endpoint initialization code whether or not this engine, in its
        current configuration, complies with Globus's High Assurance policies. This
        must be evaluated on a case-by-case basis. Some example criteria:

        * All manager-worker communication happens within one host machine
        * Network traffic is encrypted
        """
        raise NotImplementedError

    @abstractmethod
    def start(
        self,
        *args,
        **kwargs,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_status_report(self) -> EPStatusReport:
        raise NotImplementedError

    def set_working_dir(self, run_dir: str | None = None):
        if not os.path.isabs(self.working_dir):
            run_dir = os.path.abspath(run_dir or os.getcwd())
            self.working_dir = os.path.join(run_dir, self.working_dir)

    def _handle_task_exception(
        self,
        task_id: uuid.UUID,
        execution_begin: TaskTransition,
        exception_history: list[BaseException],
    ) -> Result:
        """Repackage task exception to messagepack'ed bytes

        :param task_id: Upstream task identifier
        :param execution_begin: When the task was begun
        :param exception_history: List of task exceptions (from previous attempts)

        :returns: Result object, encasulating all exceptions into a string
        """
        execution_end = TaskTransition(
            timestamp=time.time_ns(),
            actor=ActorName.INTERCHANGE,
            state=TaskState.EXEC_END,
        )

        *exc_history, last_exc = exception_history
        code, user_message = get_result_error_details(last_exc)
        error_details = {"code": code, "user_message": user_message}

        if exc_history:
            exception_string = "\n".join(
                _EXC_HISTORY_TMPL.format(ndx=index, exc=get_error_string(exc=exc))
                for index, exc in itertools.chain(
                    enumerate(exc_history), (("final attempt", last_exc),)
                )
            )
        else:
            exception_string = _EXC_NO_HISTORY_TMPL.format(
                exc=get_error_string(exc=last_exc)
            )

        res = Result(
            task_id=task_id,
            data=exception_string,
            exception=exception_string,
            error_details=error_details,
            task_statuses=[execution_begin, execution_end],  # only timings we have
        )
        return res

    def _invoke_submission(
        self,
        task_fut: GCFuture,
        submission_partial: t.Callable[..., GCExecutorFuture],
        retry_count: int = 0,
        exception_history: list | None = None,
    ):
        if exception_history is None:
            exception_history = []

        exec_beg = TaskTransition(  # Reminder: used by *closure*, below
            timestamp=time.time_ns(),
            actor=ActorName.INTERCHANGE,
            state=TaskState.WAITING_FOR_LAUNCH,
        )

        def _done_cb(f: Future):
            try:
                task_fut.set_result(f.result())
            except Exception as e:
                exception_history.append(e)
                if retry_count > 0:
                    self._invoke_submission(
                        task_fut,
                        submission_partial,
                        retry_count - 1,
                        exception_history,
                    )
                else:
                    res = self._handle_task_exception(
                        task_id=task_fut.gc_task_id,
                        execution_begin=exec_beg,
                        exception_history=exception_history,
                    )
                    task_fut.set_result(messagepack.pack(res))

        self._task_id_map.pop(task_fut.executor_task_id, None)
        try:
            work_f = submission_partial()
            task_fut.executor_task_id = work_f.executor_task_id
        except Exception as e:
            work_f = GCExecutorFuture()
            work_f.set_exception(e)
        work_f.add_done_callback(_done_cb)

    def _clear_task(self, gcf: GCFuture):
        self._task_id_map.pop(gcf.executor_task_id, None)

    def _update_executor_task_id(self, gcf: GCFuture, new_val):
        self._task_id_map[new_val] = gcf

    def set_tasks_placement(
        self,
        executor_tasks: list,
        block_id: t.Any | None = None,
        job_id: t.Any | None = None,
    ):
        """
        Child classes may use this method when they have information about where tasks
        are placed.  Based off of the Parsl infrastructure choice to relay information
        about nodes at a time rather than individual tasks, set one or more task
        locations per call.

        :param executor_tasks: a list of the executor's task identifiers; these will be
            matched against the bookkeeping we have of executor task identifiers to the
            associated GCFuture.
        :param block_id: the block identifier where these tasks are running
        :param job_id: the scheduler job identifier associated with these tasks
        """
        for ex_tid in executor_tasks:
            if f := self._task_id_map.get(ex_tid):
                f.job_id = job_id
                f.block_id = block_id

    @abstractmethod
    def _submit(
        self,
        func: t.Callable,
        resource_specification: dict,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> GCExecutorFuture:
        """Subclass should use the internal execution system to implement this"""
        raise NotImplementedError()

    def _ensure_ready(self):
        """Raises a RuntimeError if engine is not started"""
        if not self._engine_ready:
            raise RuntimeError("Engine not started and cannot execute tasks")

    def submit(
        self,
        task_f: GCFuture,
        packed_task: bytes,
        resource_specification: dict,
        result_serializers: list[str] | None = None,
    ):
        """GC Endpoints should submit tasks via this method so that tasks are
        tracked properly.

        :param task_f: The future to be notified when task is complete
        :param packed_task: The payload task (function and args) to eventually invoke
        :param resource_specification: MPI resource specification
        :param result_serializers: list of import paths to serialization strategies
        """
        self._ensure_ready()
        task_f.bind("executor_task_id", self._update_executor_task_id)
        task_f.add_done_callback(self._clear_task)  # type: ignore[arg-type]

        submission_partial = functools.partial(
            self._submit,
            execute_task,
            resource_specification,
            task_f.gc_task_id,
            packed_task,
            self.endpoint_id,
            run_dir=self.working_dir,
            run_in_sandbox=self.run_in_sandbox,
            serde=self.serde,
            result_serializers=result_serializers,
        )
        self._invoke_submission(
            task_f, submission_partial, retry_count=self.max_retries_on_system_failure
        )

    @abstractmethod
    def shutdown(self, /, **kwargs) -> None:
        raise NotImplementedError()
