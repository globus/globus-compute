from __future__ import annotations

import logging
import os
import re
import shlex
import threading
import typing as t
import uuid

from globus_compute_common.messagepack.message_types import (
    EPStatusReport,
    TaskTransition,
)
from globus_compute_endpoint.engines.base import (
    GCExecutorFuture,
    GlobusComputeEngineBase,
)
from globus_compute_sdk.serialize.facade import DeserializerAllowlist
from parsl.executors.high_throughput.executor import HighThroughputExecutor
from parsl.jobs.job_status_poller import JobStatusPoller
from parsl.monitoring.types import MessageType, TaggedMonitoringMessage
from parsl.multiprocessing import SpawnQueue

logger = logging.getLogger(__name__)
# Docker, podman, and podman-hpc all use the same syntax
DOCKER_CMD_TEMPLATE = "{cmd} run {options} -v {rundir}:{rundir} -t {image} {command}"
# Apptainer and Singularity use the same syntax
APPTAINER_CMD_TEMPLATE = "{cmd} run {options} {image} {command}"
_DOCKER_TYPES = ("docker", "podman", "podman-hpc")
_APPTAINER_TYPES = ("apptainer", "singularity")
VALID_CONTAINER_TYPES = _DOCKER_TYPES + _APPTAINER_TYPES + ("custom", None)


class JobStatusPollerKwargs(t.TypedDict, total=False):
    strategy: str | None
    max_idletime: float
    strategy_period: float | int


class GlobusComputeEngine(GlobusComputeEngineBase):
    """GlobusComputeEngine is a wrapper over Parsl's HighThroughputExecutor"""

    _ExecutorClass: t.Type[HighThroughputExecutor] = HighThroughputExecutor

    def __init__(
        self,
        *args,
        label: str | None = None,
        max_retries_on_system_failure: int = 0,
        executor: t.Optional[HighThroughputExecutor] = None,
        container_type: t.Literal[VALID_CONTAINER_TYPES] = None,  # type: ignore
        container_uri: t.Optional[str] = None,
        container_cmd_options: t.Optional[str] = None,
        encrypted: bool = True,
        strategy: str | None = None,
        job_status_kwargs: t.Optional[JobStatusPollerKwargs] = None,
        run_in_sandbox: bool = False,
        allowed_serializers: DeserializerAllowlist | None = None,
        **kwargs,
    ):
        """``GlobusComputeEngine`` is a shim over `Parsl's HighThroughputExecutor
        <parslhtex_>`_, almost all of arguments are passed along, unfettered.
        Consequently, please reference `Parsl's HighThroughputExecutor <parslhtex_>`_
        documentation for a complete list of arguments; we list below only the
        arguments specific to the ``GlobusComputeEngine``.

        .. _parslhtex: https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.HighThroughputExecutor.html
        .. _parslstrategy: https://parsl.readthedocs.io/en/stable/stubs/parsl.jobs.strategy.Strategy.html
        .. _parsljobstatuspoller: https://parsl.readthedocs.io/en/stable/stubs/parsl.jobs.job_status_poller.JobStatusPoller.html

        Parameters
        ----------

        label: str | None
           Label used to name engine log directories and batch jobs
           default: None (in which case, "GlobusComputeEngine" will be set)

        max_retries_on_system_failure: int
           Set the number of retries for functions that fail due to
           system failures such as node failure/loss. Since functions
           can fail after partial runs, consider additional cleanup
           logic before enabling this functionality
           default: 0

        strategy: str | None
            Specify which scaling strategy to use; this is eventually given to
            `Parsl's Strategy <parslstrategy_>`_.  [Deprecated; use
            ``job_status_kwargs``]

        job_status_kwargs: dict | None
            Keyword arguments to be passed through to `Parsl's JobStatusPoller
            <parsljobstatuspoller_>`_ class that drives strategy to do auto-scaling.

        encrypted: bool
            Flag to enable/disable encryption (CurveZMQ). Default is True.

        run_in_sandbox: bool
            Functions will run in a sandbox directory under the working_dir
            if this option is enabled. Default: False

        allowed_serializers: DeserializerAllowlist | None
            If specified, any submissions that were not serialized using the
            strategies given in this list will cause an error to be raised.
            Passed to the engine's ``ComputeSerializer`` as ``allowed_deserializers``.

        """  # noqa: E501

        if self._ExecutorClass is HighThroughputExecutor:
            # temporary measure to help users upgrade
            if kwargs.get("enable_mpi_mode") or kwargs.get("mpi_launcher"):
                raise ValueError(
                    "MPI mode is not supported on GlobusComputeEngine."
                    " Use GlobusMPIEngine instead."
                )

        ex_name = self._ExecutorClass.__name__
        self.run_dir = os.getcwd()
        self.label = label or type(self).__name__
        super().__init__(
            *args,
            max_retries_on_system_failure=max_retries_on_system_failure,
            allowed_serializers=allowed_serializers,
            **kwargs,
        )
        self.strategy = strategy

        self.container_type = container_type
        assert (
            self.container_type in VALID_CONTAINER_TYPES
        ), f"{self.container_type} is not a valid container_type"
        self.container_uri = container_uri
        self.container_cmd_options = container_cmd_options

        if executor is None:
            executor = self._ExecutorClass(
                *args,
                **{
                    **kwargs,
                    "label": f"{self.label}-{ex_name}",
                    "encrypted": encrypted,
                },
            )
        elif not isinstance(executor, self._ExecutorClass):
            # Mypy: nominally unreachable, but humans can typo before they get it
            # right; help the hapless typist out with a pointed error message
            found = type(executor).__name__  # type: ignore[unreachable]
            raise TypeError(f"`executor` must of type {ex_name} (received: {found})")

        self.executor = executor
        self.executor.interchange_launch_cmd = self._get_compute_ix_launch_cmd()
        self.executor.launch_cmd = self._get_compute_launch_cmd()

        self.run_in_sandbox = run_in_sandbox
        if strategy is None:
            strategy = "simple"

        # Set defaults for JobStatusPoller
        self._job_status_kwargs: JobStatusPollerKwargs = {
            "max_idletime": 120.0,
            "strategy": strategy,
            "strategy_period": 5.0,
        }
        self._job_status_kwargs.update(job_status_kwargs or {})
        self.job_status_poller: JobStatusPoller | None = None
        # N.B. call `.add_executor()` *after* starting executor; see .start()

    def assert_ha_compliant(self):
        if not self.encrypted:
            raise ValueError(
                "High-Assurance GlobusComputeEngines must enable the"
                " `encrypted` configuration value"
            )

    @property
    def max_workers_per_node(self):
        # Needed for strategies (e.g., SimpleStrategy)
        return self.executor.max_workers_per_node

    @property
    def encrypted(self):
        return self.executor.encrypted

    def _get_compute_ix_launch_cmd(self) -> t.List[str]:
        """Returns an endpoint CLI command to launch Parsl's interchange process,
        rather than rely on Parsl's interchange.py command. This helps to minimize
        PATH-related issues.
        """
        return [
            "globus-compute-endpoint",
            "python-exec",
            "parsl.executors.high_throughput.interchange",
        ]

    def _get_compute_launch_cmd(self) -> str:
        """Modify the launch command so that we launch the worker pool via the
        endpoint CLI, rather than Parsl's process_worker_pool.py command. This
        helps to minimize PATH-related issues.
        """
        launch_cmd = self.executor.launch_cmd
        launch_cmd = re.sub(
            r"^process_worker_pool\.py",
            (
                "globus-compute-endpoint python-exec"
                " parsl.executors.high_throughput.process_worker_pool"
            ),
            launch_cmd,
        )
        launch_cmd = " ".join(shlex.split(launch_cmd))
        return launch_cmd

    def containerized_launch_cmd(self) -> str:
        """Recompose executor's launch_cmd to launch with containers

        Returns
        -------
        str launch_cmd
        """
        launch_cmd = self.executor.launch_cmd
        # Adding assert here since mypy can't figure out launch_cmd's type
        assert launch_cmd
        if self.container_type in _DOCKER_TYPES:
            launch_cmd = DOCKER_CMD_TEMPLATE.format(
                cmd=self.container_type,
                image=self.container_uri,
                rundir=self.run_dir,
                command=launch_cmd,
                options=self.container_cmd_options or "",
            )
        elif self.container_type in _APPTAINER_TYPES:
            launch_cmd = APPTAINER_CMD_TEMPLATE.format(
                cmd=self.container_type,
                image=self.container_uri,
                command=launch_cmd,
                options=self.container_cmd_options or "",
            )
        elif self.container_type == "custom":
            assert (
                self.container_cmd_options
            ), "GCE.container_cmd_options is required for GCE.container_type=custom"
            template = self.container_cmd_options.replace(
                "{EXECUTOR_RUNDIR}", str(self.run_dir)
            )
            launch_cmd = template.replace("{EXECUTOR_LAUNCH_CMD}", launch_cmd)

        # Remove extra whitespace between tokens
        launch_cmd = " ".join(shlex.split(launch_cmd))
        return launch_cmd

    def monitor_watcher(self) -> None:
        logger.debug("Thread start")

        monitoring_q = self.executor.monitoring_messages
        if not monitoring_q:
            logger.debug("Monitoring not enabled; thread exit")
            return

        while True:
            msg: TaggedMonitoringMessage | None = monitoring_q.get()
            if msg is None:  # poison pill
                break

            try:
                msg_type: MessageType
                data: list[dict] | dict
                msg_type, data = msg
            except Exception as e:
                logger.debug(
                    "Unexpected monitoring data structure: [%s] %s -- (msg: %s)",
                    type(e).__name__,
                    e,
                    msg,
                )
                continue

            try:
                if msg_type == MessageType.NODE_INFO:
                    if bid := data.get("block_id"):
                        jid = self.executor.blocks_to_job_id.get(bid)
                        if tasks := data.get("tasks"):
                            self.set_tasks_placement(
                                executor_tasks=tasks, block_id=bid, job_id=jid
                            )

            except Exception as e:
                logger.error(
                    "Unexpected monitoring message structure: [%s] %s -- (data: %s)",
                    type(e).__name__,
                    e,
                    data,
                )

        logger.debug("Thread exit")

    def start(
        self,
        *args,
        endpoint_id: t.Optional[uuid.UUID] = None,
        run_dir: t.Optional[str] = None,
        **kwargs,
    ):
        assert endpoint_id, "GCExecutor requires kwarg:endpoint_id at start"
        assert run_dir, "GCExecutor requires kwarg:run_dir at start"

        self.set_working_dir(run_dir=run_dir)

        self.endpoint_id = endpoint_id
        self.run_dir = run_dir
        self.executor.run_dir = self.run_dir
        script_dir = os.path.join(self.run_dir, "submit_scripts")
        self.executor.provider.script_dir = script_dir
        if self.container_type:
            self.executor.launch_cmd = self.containerized_launch_cmd()
            logger.info(
                f"Containerized launch cmd template: {self.executor.launch_cmd}"
            )

        os.makedirs(self.executor.provider.script_dir, exist_ok=True)

        # A minor amout of black magic because we're not using Parsl's DFK:
        # attach the `monitoring_messages` queue to the executor before starting
        # the executor but *after* the GC Endpoint will have detached.  The
        # existence of `monitoring_messages` tells Parsl to enable the requisite
        # machinery to give us such details as `block_id`.
        self.executor.monitoring_messages = SpawnQueue()

        threading.Thread(target=self.monitor_watcher, daemon=True).start()

        self.executor.start()

        # Add executor to poller *after* executor has started
        self.job_status_poller = JobStatusPoller(**self._job_status_kwargs)
        self.job_status_poller.add_executors([self.executor])
        self._engine_ready = True

    def _submit(
        self,
        func: t.Callable,
        resource_specification: t.Dict,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> GCExecutorFuture:
        if resource_specification:
            raise ValueError(
                "resource_specification is not supported on GlobusComputeEngine."
                " For MPI apps, use GlobusMPIEngine."
            )
        f = t.cast(
            GCExecutorFuture,
            self.executor.submit(func, resource_specification, *args, **kwargs),
        )
        f.executor_task_id = f.parsl_executor_task_id  # type: ignore[attr-defined]
        return f

    @property
    def provider(self):
        return self.executor.provider

    def get_connected_managers(self) -> t.List[t.Dict[str, t.Any]]:
        """
        Returns
        -------
        List of dicts containing info for all connected managers
        """
        return self.executor.connected_managers()

    def get_connected_managers_packages(self) -> t.Dict[str, t.Dict[str, str]]:
        """
        Returns
        -------
        Dict mapping each connected manager ID to a dict of installed packages
        and versions
        """
        return self.executor.connected_managers_packages()

    def get_total_managers(self, managers: t.List[t.Dict[str, t.Any]]) -> int:
        """
        Parameters
        ----------
        managers: list[dict[str, Any]]
            List of dicts containing info for all connected managers

        Returns
        -------
        Total number of connected managers
        """
        return len(managers)

    def get_total_active_managers(self, managers: t.List[t.Dict[str, t.Any]]) -> int:
        """
        Parameters
        ----------
        managers: list[dict[str, Any]]
            List of dicts containing info for all connected managers

        Returns
        -------
        Number of managers that are accepting new tasks
        """
        return sum(1 for m in managers if m["active"])

    def get_outstanding_breakdown(
        self, managers: t.Optional[t.List[t.Dict[str, t.Any]]] = None
    ) -> t.List[t.Tuple[str, int, bool]]:
        """
        Parameters
        ----------
        managers: list[dict[str, Any]] | None
            List of dicts containing info for all connected managers

        Returns
        -------
        List of tuples of the form (component, # of tasks on component, active?)
        """
        if managers is None:
            managers = self.get_connected_managers()
        total_task_count = self.executor.outstanding
        breakdown = [(m["manager"], m["tasks"], m["active"]) for m in managers]
        total_count_managers = sum([m["tasks"] for m in managers])
        task_count_interchange = total_task_count - total_count_managers
        breakdown = [("interchange", task_count_interchange, True)] + breakdown
        return breakdown

    def get_total_tasks_outstanding(self) -> dict:
        """
        Returns
        -------
        Dict of type {str_task_type: count_tasks}
        """
        return {"RAW": self.executor.outstanding}

    def get_total_tasks_pending(self, managers: t.List[t.Dict[str, t.Any]]) -> int:
        """
        Parameters
        ----------
        managers: list[dict[str, Any]]
            List of dicts containing info for all connected managers

        Returns
        -------
        Total number of tasks that are queued in the executor's interchange
        """
        outstanding = self.get_outstanding_breakdown(managers=managers)
        return outstanding[0][1]  # Queued in interchange

    def provider_status(self):
        status = []
        if self.provider:
            # ex.locks is a dict of block_id:job_id mappings
            job_ids = self.executor.blocks.values()
            status = self.provider.status(job_ids=job_ids)
        return status

    def get_total_live_workers(
        self, managers: t.Optional[t.List[t.Dict[str, t.Any]]] = None
    ) -> int:
        """
        Parameters
        ----------
        managers: list[dict[str, Any]]
            List of dicts containing info for all connected managers

        Returns
        -------
        Total number of workers that are actively running tasks
        """
        if managers is None:
            managers = self.get_connected_managers()
        return sum([mgr["worker_count"] for mgr in managers])

    def get_total_idle_workers(self, managers: t.List[t.Dict[str, t.Any]]) -> int:
        """
        Parameters
        ----------
        managers: list[dict[str, Any]]
            List of dicts containing info for all connected managers

        Returns
        -------
        Total number of workers that are not actively running tasks
        """
        idle_workers = 0
        for mgr in managers:
            workers = mgr["worker_count"]
            tasks = mgr["tasks"]
            idle_workers += max(0, workers - tasks)
        return idle_workers

    def scale_out(self, blocks: int):
        logger.info(f"Scaling out {blocks} blocks")
        return self.executor.scale_out_facade(n=blocks)

    def scale_in(self, blocks: int):
        logger.info(f"Scaling in {blocks} blocks")
        return self.executor.scale_in_facade(n=blocks)

    @property
    def scaling_enabled(self) -> bool:
        """Indicates whether scaling is possible"""
        max_blocks = self.executor.provider.max_blocks
        return max_blocks > 0

    def _get_provider_attr(self, possible_names: t.Tuple[str, ...]) -> str | None:
        for name in possible_names:
            value = getattr(self.executor.provider, name, None)
            if value:
                return value
        return None

    def get_status_report(self) -> EPStatusReport:
        """
        Returns
        -------
        Object containing info on the current status of the endpoint
        """
        managers = self.get_connected_managers()
        managers_packages = self.get_connected_managers_packages()
        manager_info: dict[str, list] = {}
        for m in managers:
            jid = self.executor.blocks_to_job_id[m["block_id"]]
            packages = managers_packages[m["manager"]]
            m_info = {
                "worker_count": m["worker_count"],
                "idle_duration": m["idle_duration"],
                "parsl_version": m["parsl_version"],
                "endpoint_version": packages.get("globus-compute-endpoint"),
                "sdk_version": packages.get("globus-compute-sdk"),
                "python_version": m["python_version"],
            }
            manager_info.setdefault(jid, []).append(m_info)

        executor_status: t.Dict[str, t.Any] = {
            "managers": self.get_total_managers(managers=managers),  # aka: nodes
            "active_managers": self.get_total_active_managers(managers=managers),
            "total_workers": self.get_total_live_workers(managers=managers),
            "idle_workers": self.get_total_idle_workers(managers=managers),
            "pending_tasks": self.get_total_tasks_pending(managers=managers),
            "outstanding_tasks": self.get_total_tasks_outstanding()["RAW"],
            "total_tasks": self.executor._task_counter,
            "scaling_enabled": self.scaling_enabled,
            "mem_per_worker": self.executor.mem_per_worker,
            "cores_per_worker": self.executor.cores_per_worker,
            "prefetch_capacity": self.executor.prefetch_capacity,
            "max_blocks": self.executor.provider.max_blocks,
            "min_blocks": self.executor.provider.min_blocks,
            "max_workers_per_node": self.executor.max_workers_per_node,
            "nodes_per_block": self.executor.provider.nodes_per_block,
            "node_info": manager_info,
            "engine_type": type(self).__name__,
            "provider_type": type(self.executor.provider).__name__,
            "queue": self._get_provider_attr(("queue", "partition")),
            "account": self._get_provider_attr(("account", "allocation", "project")),
        }
        task_status_deltas: t.Dict[str, t.List[TaskTransition]] = {}  # TODO
        return EPStatusReport(
            endpoint_id=self.endpoint_id,
            global_state=executor_status,
            task_statuses=task_status_deltas,
        )

    @property
    def executor_exception(self) -> t.Optional[Exception]:
        return self.executor.executor_exception

    def shutdown(self, /, **kwargs) -> None:
        if self.executor.monitoring_messages:
            self.executor.monitoring_messages.put(None)  # type: ignore[arg-type]

        if self.job_status_poller:
            self.job_status_poller.close()
            self.job_status_poller = None

        self.executor.shutdown()
