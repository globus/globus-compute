from __future__ import annotations

import logging
import multiprocessing
import typing as t
import uuid
from concurrent.futures import ThreadPoolExecutor as NativeExecutor

import psutil
from globus_compute_common.messagepack.message_types import (
    EPStatusReport,
    TaskTransition,
)
from globus_compute_endpoint.engines.base import (
    GCExecutorFuture,
    GlobusComputeEngineBase,
)
from globus_compute_sdk.serialize.facade import DeserializerAllowlist

logger = logging.getLogger(__name__)


class ThreadPoolEngine(GlobusComputeEngineBase):
    def __init__(
        self,
        *args,
        label: str = "ThreadPoolEngine",
        allowed_serializers: DeserializerAllowlist | None = None,
        **kwargs,
    ):
        self.label = label
        self.executor = NativeExecutor(*args, **kwargs)
        self._task_counter: int = 0
        super().__init__(
            *args,
            **kwargs,
            allowed_serializers=allowed_serializers,
        )

    def assert_ha_compliant(self):
        # HA compliant by default
        pass

    def start(
        self,
        *args,
        endpoint_id: t.Optional[uuid.UUID] = None,
        run_dir: t.Optional[str] = None,
        **kwargs,
    ) -> None:
        """
        Parameters
        ----------
        endpoint_id: Endpoint UUID
        run_dir: endpoint run directory
        Returns
        -------
        """
        assert endpoint_id, "ThreadPoolEngine requires kwarg:endpoint_id at start"
        self.endpoint_id = endpoint_id
        self.set_working_dir(run_dir=run_dir)
        # mypy think the thread can be none
        self._engine_ready = True

    def get_status_report(self) -> EPStatusReport:
        executor_status: t.Dict[str, t.Any] = {
            "total_cores": multiprocessing.cpu_count(),
            "total_mem": round(psutil.virtual_memory().available / (2**30), 1),
            "total_core_hrs": 0,
            "total_workers": self.executor._max_workers,  # type: ignore
            "pending_tasks": 0,
            "outstanding_tasks": 0,
            "scaling_enabled": False,
            "max_blocks": 1,
            "min_blocks": 1,
            "max_workers_per_node": self.executor._max_workers,  # type: ignore
            "nodes_per_block": 1,
            "engine_type": type(self).__name__,
        }
        task_status_deltas: t.Dict[str, t.List[TaskTransition]] = {}

        return EPStatusReport(
            endpoint_id=self.endpoint_id,
            global_state=executor_status,
            task_statuses=task_status_deltas,
        )

    def _submit(
        self,
        func: t.Callable,
        resource_specification: t.Dict,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> GCExecutorFuture:
        """``resource_specification`` is not applicable to the ThreadPoolEngine"""

        self._task_counter += 1
        f = t.cast(GCExecutorFuture, self.executor.submit(func, *args, **kwargs))
        f.executor_task_id = self._task_counter
        return f

    def scale_out(self, blocks: int) -> list[str]:
        return []

    def scale_in(self, blocks: int) -> list[str]:
        return []

    def status(self) -> dict:
        return {}

    def shutdown(self, /, block=False, **kwargs) -> None:
        self.executor.shutdown(wait=block)
