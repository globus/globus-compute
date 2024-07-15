import os
import typing as t
from concurrent.futures import Future

from globus_compute_endpoint.engines.globus_compute import (
    VALID_CONTAINER_TYPES,
    GlobusComputeEngine,
    JobStatusPollerKwargs,
)
from parsl.executors import MPIExecutor


class GlobusMPIEngine(GlobusComputeEngine):

    def __init__(
        self,
        *args,
        label: str = "GlobusMPIEngine",
        max_retries_on_system_failure: int = 0,
        executor: t.Optional[MPIExecutor] = None,
        container_type: t.Literal[VALID_CONTAINER_TYPES] = None,  # type: ignore
        container_uri: t.Optional[str] = None,
        container_cmd_options: t.Optional[str] = None,
        encrypted: bool = True,
        strategy: t.Optional[str] = None,
        job_status_kwargs: t.Optional[JobStatusPollerKwargs] = None,
        working_dir: t.Union[str, os.PathLike] = "tasks_working_dir",
        run_in_sandbox: bool = True,
        **kwargs,
    ):
        """``GlobusMPIEngine`` is a shim over `Parsl's MPIExecutor
        <parslmpiex_>`_, and as such, all of arguments are passed along, unfettered.
        Consequently, please reference `Parsl's MPIExecutor <parslmpiex_>`_
        documentation for a complete list of arguments; we list below only the
        arguments specific to the ``GlobusMPIEngine``.

        .. _parslmpiex: https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.MPIExecutor.html
        .. _parslstrategy: https://parsl.readthedocs.io/en/stable/stubs/parsl.jobs.strategy.Strategy.html
        .. _parsljobstatuspoller: https://parsl.readthedocs.io/en/stable/stubs/parsl.jobs.job_status_poller.JobStatusPoller.html

        Parameters
        ----------

        label: str
           Label used to name engine log directories and batch jobs
           default: "GlobusComputeEngine"

        max_retries_on_system_failure: int
           Set the number of retries for functions that fail due to
           system failures such as node failure/loss. Since functions
           can fail after partial runs, consider additional cleanup
           logic before enabling this functionality
           default: 0

        strategy: str | None
            Specify which scaling strategy to use; this is eventually given to
            `Parsl's Strategy <parslstrategy_>`_.

        job_status_kwargs: dict | None
            Keyword arguments to be passed through to `Parsl's JobStatusPoller
            <parsljobstatuspoller_>`_ class that drives strategy to do auto-scaling.

        encrypted: bool
            Flag to enable/disable encryption (CurveZMQ). Default is True.

        working_dir: str | os.PathLike
            Directory within which functions should execute, defaults to
            ``~/.globus_compute/<endpoint_name>/tasks_working_dir``.
            If a relative path is supplied, the working dir is set relative
            to the ``endpoint.run_dir``. If an absolute path is supplied, it is
            used as is.

        run_in_sandbox: bool
            Functions will run in a sandbox directory under the ``working_dir``
            if this option is enabled. Default: True

        """  # noqa: E501

        if executor is None:
            executor = MPIExecutor(  # type: ignore
                *args,
                label=label,
                encrypted=encrypted,
                **kwargs,
            )
        elif not isinstance(executor, MPIExecutor):
            raise TypeError(
                "The executor must be an instance of parsl.executors.MPIExecutor"
            )

        super().__init__(
            label=label,
            max_retries_on_system_failure=max_retries_on_system_failure,
            executor=executor,
            container_type=container_type,
            container_uri=container_uri,
            container_cmd_options=container_cmd_options,
            encrypted=encrypted,
            strategy=strategy,
            job_status_kwargs=job_status_kwargs,
            working_dir=working_dir,
            run_in_sandbox=run_in_sandbox,
        )

    def _submit(
        self,
        func: t.Callable,
        resource_specification: t.Dict,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> Future:
        # override submit since super rejects resource_specification
        return self.executor.submit(func, resource_specification, *args, **kwargs)
