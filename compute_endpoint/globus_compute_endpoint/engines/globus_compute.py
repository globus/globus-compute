import logging
import os
import queue
import typing as t
import uuid
from concurrent.futures import Future

from globus_compute_common.messagepack.message_types import (
    EPStatusReport,
    TaskTransition,
)
from globus_compute_endpoint.engines.base import (
    GlobusComputeEngineBase,
    ReportingThread,
)
from globus_compute_endpoint.strategies import SimpleStrategy
from parsl.executors.high_throughput.executor import HighThroughputExecutor

logger = logging.getLogger(__name__)


class GlobusComputeEngine(GlobusComputeEngineBase):
    def __init__(
        self,
        *args,
        label: str = "GlobusComputeEngine",
        address: t.Optional[str] = None,
        heartbeat_period_s: float = 30.0,
        strategy: t.Optional[SimpleStrategy] = SimpleStrategy(),
        **kwargs,
    ):
        self.address = address
        self.run_dir = os.getcwd()
        self.label = label
        self._status_report_thread = ReportingThread(
            target=self.report_status, args=[], reporting_period=heartbeat_period_s
        )
        super().__init__(*args, heartbeat_period_s=heartbeat_period_s, **kwargs)
        self.strategy = strategy
        self.executor = HighThroughputExecutor(  # type: ignore
            *args, address=address, **kwargs
        )
        self.max_workers_per_node = 1

    def start(
        self,
        *args,
        endpoint_id: t.Optional[uuid.UUID] = None,
        run_dir: t.Optional[str] = None,
        results_passthrough: t.Optional[queue.Queue] = None,
        **kwargs,
    ):
        assert run_dir, "GCExecutor requires kwarg:run_dir at start"
        assert endpoint_id, "GCExecutor requires kwarg:endpoint_id at start"
        self.run_dir = os.path.join(os.getcwd(), run_dir)
        self.endpoint_id = endpoint_id
        self.executor.provider.script_dir = os.path.join(self.run_dir, "submit_scripts")
        os.makedirs(self.executor.provider.script_dir, exist_ok=True)
        if results_passthrough:
            # Only update the default queue in GCExecutorBase if
            # a queue is passed in
            self.results_passthrough = results_passthrough
        self.executor.start()
        if self.strategy:
            self.strategy.start(self)
        self._status_report_thread.start()

    def _submit(
        self,
        func: t.Callable,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> Future:
        return self.executor.submit(func, {}, *args, **kwargs)

    @property
    def provider(self):
        return self.executor.provider

    def get_outstanding_breakdown(self) -> t.List[t.Tuple[str, int, bool]]:
        """

        Returns
        -------
        List of tuples of the form (component, # of tasks on component, active?)
        """
        total_task_count = self.executor.outstanding
        manager_info: t.List[t.Dict[str, t.Any]] = self.executor.connected_managers()
        breakdown = [(m["manager"], m["tasks"], m["active"]) for m in manager_info]
        total_count_managers = sum([m["tasks"] for m in manager_info])
        task_count_interchange = total_task_count - total_count_managers
        breakdown = [("interchange", task_count_interchange, True)] + breakdown
        return breakdown

    def get_total_tasks_outstanding(self):
        """

        Returns
        -------
        Returns a dict of type {str_task_type: count_tasks}

        """
        outstanding = self.get_outstanding_breakdown()
        total = sum([component[1] for component in outstanding])
        return {"RAW": total}

    def provider_status(self):
        status = []
        if self.provider:
            # ex.locks is a dict of block_id:job_id mappings
            job_ids = self.executor.blocks.values()
            status = self.provider.status(job_ids=job_ids)
        return status

    def get_total_live_workers(self) -> int:
        manager_info: t.List[dict[str, t.Any]] = self.executor.connected_managers()
        worker_count = sum([mgr["worker_count"] for mgr in manager_info])
        return worker_count

    def scale_out(self, blocks: int):
        logger.info(f"Scaling out {blocks} blocks")
        return self.executor.scale_out(blocks=blocks)

    def scale_in(self, blocks: int):
        logger.info(f"Scaling in {blocks} blocks")
        to_kill = list(self.executor.blocks.values())[:blocks]
        return self.provider.cancel(to_kill)

    def get_status_report(self) -> EPStatusReport:
        """
        endpoint_id: uuid.UUID
        ep_status_report: t.Dict[str, t.Any]
        task_statuses: t.Dict[str, t.List[TaskTransition]]
        Returns
        -------
        """
        executor_status: t.Dict[str, t.Any] = {
            "task_id": -2,
            "info": {
                "total_cores": 0,
                "total_mem": 0,
                "new_core_hrs": 0,
                "total_core_hrs": 0,
                "managers": 0,
                "active_managers": 0,
                "total_workers": 0,
                "idle_workers": 0,
                "pending_tasks": 0,
                "outstanding_tasks": 0,
                "worker_mode": 0,
                "scheduler_mode": 0,
                "scaling_enabled": False,
                "mem_per_worker": 0,
                "cores_per_worker": 0,
                "prefetch_capacity": 0,
                "max_blocks": 1,
                "min_blocks": 1,
                "max_workers_per_node": 0,
                "nodes_per_block": 1,
                "heartbeat_period": self._heartbeat_period_s,
            },
        }
        task_status_deltas: t.Dict[str, t.List[TaskTransition]] = {}
        return EPStatusReport(
            endpoint_id=self.endpoint_id,
            global_state=executor_status,
            task_statuses=task_status_deltas,
        )

    def shutdown(self):
        self._status_report_thread.stop()
        if self.strategy:
            self.strategy.close()
        return self.executor.shutdown()
