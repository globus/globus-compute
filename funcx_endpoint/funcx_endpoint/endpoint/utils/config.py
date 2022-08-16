from __future__ import annotations

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
        results_ws_uri unless they are also specified.
        Default: "prod"

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
    """

    def __init__(
        self,
        # Execution backed
        executors: list = _DEFAULT_EXECUTORS,
        # Connection info
        environment: str | None = None,
        funcx_service_address=None,
        results_ws_uri=None,
        warn_about_url_mismatch=False,
        # Tuning info
        heartbeat_period=30,
        heartbeat_threshold=120,
        detach_endpoint=True,
        # Logging info
        log_dir=None,
        stdout="./endpoint.log",
        stderr="./endpoint.log",
    ):

        # Execution backends
        self.executors = executors  # List of executors

        # Connection info
        self.environment = environment
        self.funcx_service_address = funcx_service_address
        self.results_ws_uri = results_ws_uri
        self.warn_about_url_mismatch = warn_about_url_mismatch

        # Tuning info
        self.heartbeat_period = heartbeat_period
        self.heartbeat_threshold = heartbeat_threshold
        self.detach_endpoint = detach_endpoint

        # Logging info
        self.log_dir = log_dir
        self.stdout = stdout
        self.stderr = stderr
