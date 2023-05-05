from __future__ import annotations

import inspect
import warnings

from globus_compute_endpoint.executors import HighThroughputExecutor
from parsl.utils import RepresentationMixin

_DEFAULT_EXECUTORS = [HighThroughputExecutor()]


class Config(RepresentationMixin):
    """Specification of Globus Compute configuration options.

    Parameters
    ----------

    executors : list of Executors
        A list of executors which serve as the backend for function execution.
        As of 0.2.2, this list should contain only one executor.
        Default: [HighThroughtputExecutor()]

    environment: str
        Environment the endpoint should connect to. Sets funcx_service_address and
        results_ws_uri unless they are also specified. If not specified, the endpoint
        connects to production.
        Default: None

    funcx_service_address: str | None
        URL address string of the Globus Compute service to which the Endpoint
        should connect.
        Default: None

    results_ws_uri: str | None
        URL address string of the Globus Compute websocket service passed to the
        Globus Compute client.
        Default: None

    warn_about_url_mismatch: Bool
        Log a warning if funcx_service_address and results_ws_uri appear to point to
        different environments.
        Default: False

    heartbeat_period: int (seconds)
        The interval at which heartbeat messages are sent from the endpoint to the
        Globus Compute web service
        Default: 30s

    heartbeat_threshold: int (seconds)
        Seconds since the last hearbeat message from the Globus Compute web service
        after which the connection is assumed to be disconnected.
        Default: 120s

    idle_heartbeats_soft: int (count)
        Number of heartbeats after an endpoint is idle (no outstanding tasks or
        results, and at least 1 task or result has been forwarded) before the
        endpoint shuts down.  If 0, then the endpoint must be manually triggered
        to shut down (e.g., SIGINT or SIGTERM).
        Default: 0

    idle_heartbeats_hard: int (count)
        Number of heartbeats after no task or result has moved before the endpoint
        shuts down.  Unlike `idle_heartbeats_soft`, this idle timer does not require
        that there are no outstanding tasks or results.  If no task or result has
        moved in this many heartbeats, then the endpoint will shut down.  In
        particular, this is intended to catch the error condition that a worker
        has gone missing, and will thus _never_ return the task it was sent.
        Note that this setting is only enabled if the `idle_heartbeats_soft` is a
        value greater than 0.  Suggested value: a multiplier of heartbeat_period
        equivalent to two days.  For example, if `heartbeat_period` is 30s, then
        suggest 5760.
        Default: 5760

    stdout : str
        Path where the endpoint's stdout should be written
        Default: ./interchange.stdout

    stderr : str
        Path where the endpoint's stderr should be written
        Default: ./interchange.stderr

    detach_endpoint : Bool
        Should the endpoint deamon be run as a detached process? This is good for
        a real edge node, but an anti-pattern for kubernetes pods
        Default: True

    log_dir : str
        Optional path string to the top-level directory where logs should be written to.
        Default: None

    multi_tenant : bool | None
        Designates the endpoint as a multi-tenant endpoint
        Default: None

    display_name : str | None
        The display name for the endpoint.  If None, defaults to name
        Default: None
    """

    def __init__(
        self,
        # Execution backed
        executors: list = _DEFAULT_EXECUTORS,
        # Connection info
        environment: str | None = None,
        funcx_service_address=None,
        multi_tenant: bool | None = None,
        allowed_functions: list[str] | None = None,
        # Tuning info
        heartbeat_period=30,
        heartbeat_threshold=120,
        idle_heartbeats_soft=0,
        idle_heartbeats_hard=5760,  # Two days, divided by `heartbeat_period`
        detach_endpoint=True,
        # Misc info
        display_name: str | None = None,
        # Logging info
        log_dir=None,
        stdout="./endpoint.log",
        stderr="./endpoint.log",
        **kwargs,
    ):
        for deprecated_name in ("results_ws_uri", "warn_about_url_mismatch"):
            if deprecated_name in kwargs:
                caller_filename = inspect.stack()[1].filename
                msg = (
                    f"The '{deprecated_name}' argument is deprecated.  It will"
                    f" be removed in a future release.  Found in: {caller_filename}"
                )
                warnings.warn(msg)
                del kwargs[deprecated_name]

        for unknown_arg in kwargs:
            # Calculated multiple times, but only in errant case -- nominally
            # fixed and then "not an issue."
            caller_filename = inspect.stack()[1].filename
            cls_name = self.__class__.__name__
            msg = (
                f"Unknown argument to {cls_name} ignored: {unknown_arg}"
                f"\n  Specified in: {caller_filename}"
            )
            warnings.warn(msg)

        # Execution backends
        self.executors = executors  # List of executors

        # Connection info
        self.environment = environment
        self.funcx_service_address = funcx_service_address

        self.multi_tenant = multi_tenant is True
        self.allowed_functions = allowed_functions

        # Tuning info
        self.heartbeat_period = heartbeat_period
        self.heartbeat_threshold = heartbeat_threshold
        self.idle_heartbeats_soft = int(max(0, idle_heartbeats_soft))
        self.idle_heartbeats_hard = int(max(0, idle_heartbeats_hard))
        self.detach_endpoint = detach_endpoint

        # Logging info
        self.log_dir = log_dir
        self.stdout = stdout
        self.stderr = stderr

        # Misc info
        self.display_name = display_name
