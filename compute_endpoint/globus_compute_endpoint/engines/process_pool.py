from __future__ import annotations

import logging
import multiprocessing
import typing as t
import uuid
from concurrent.futures import ProcessPoolExecutor as NativeExecutor

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


class ProcessPoolEngine(GlobusComputeEngineBase):
    def __init__(
        self,
        *args,
        label: str = "ProcessPoolEngine",
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
        assert endpoint_id, "ProcessPoolEngine requires kwarg:endpoint_id at start"
        self.endpoint_id = endpoint_id
        self.set_working_dir(run_dir=run_dir)

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
        resource_specification: t.Dict,
        func: t.Callable,
        packed_task: bytes,
        args: tuple[t.Any, ...] = (),
        kwargs: dict | None = None,
    ) -> GCExecutorFuture:
        if resource_specification:
            raise ValueError(
                f"resource_specification is not supported on {type(self).__name__}."
                "  For MPI apps, use GlobusMPIEngine."
            )

        assert self.executor is not None, "We gotchu, mypy"

        self._task_counter += 1
        _f = self.executor.submit(func, packed_task, *args, **(kwargs or {}))
        f = t.cast(GCExecutorFuture, _f)
        f.executor_task_id = self._task_counter
        return f

    def shutdown(self, /, block=False, **kwargs) -> None:
        self.executor.shutdown(wait=block)
