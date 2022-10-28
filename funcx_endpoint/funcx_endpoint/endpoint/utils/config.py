from __future__ import annotations

import inspect
import warnings

from parsl.utils import RepresentationMixin

from funcx_endpoint.executors import HighThroughputExecutor

_DEFAULT_EXECUTORS = [HighThroughputExecutor()]


class Config(RepresentationMixin):
    """Specification of FuncX configuration options.

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
        URL address string of the funcX service to which the Endpoint should connect.
        Default: None

    results_ws_uri: str | None
        URL address string of the funcX websocket service passed to the funcX client.
        Default: None

    warn_about_url_mismatch: Bool
        Log a warning if funcx_service_address and results_ws_uri appear to point to
        different environments.
        Default: False

    heartbeat_period: int (seconds)
        The interval at which heartbeat messages are sent from the endpoint to the
        funcx-web-service
        Default: 30s

    heartbeat_threshold: int (seconds)
        Seconds since the last hearbeat message from the funcx-web-service after which
        the connection is assumed to be disconnected.
        Default: 120s

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
    """

    def __init__(
        self,
        # Execution backed
        executors: list = _DEFAULT_EXECUTORS,
        # Connection info
        environment: str | None = None,
        funcx_service_address=None,
        multi_tenant: bool | None = None,
        # Tuning info
        heartbeat_period=30,
        heartbeat_threshold=120,
        detach_endpoint=True,
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

        # Tuning info
        self.heartbeat_period = heartbeat_period
        self.heartbeat_threshold = heartbeat_threshold
        self.detach_endpoint = detach_endpoint

        # Logging info
        self.log_dir = log_dir
        self.stdout = stdout
        self.stderr = stderr
