from parsl.utils import RepresentationMixin
from parsl.providers import LocalProvider
from funcx_endpoint.strategies.simple import SimpleStrategy
from funcx_endpoint.executors import HighThroughputExecutor


class Config(RepresentationMixin):
    """ Specification of FuncX configuration options.

    Parameters
    ----------

    max_workers_per_node : int
        Maximum # of worker per node. Default: inf

    cores_per_worker : float
        cores to be assigned to each worker. Oversubscription is possible
        by setting cores_per_worker < 1.0. Default=1

    mem_per_worker : float
        GB of memory required per worker. If this option is specified, the node manager
        will check the available memory at startup and limit the number of workers such that
        the there's sufficient memory for each worker. Default: None

    working_dir : str
        Working dir to be used by the executor. Default to the endpoint directory if not specified

    worker_debug : Bool
        Enables worker debug logging.

    worker_mode : str
        Select the mode of operation from no_container, singularity_reuse, singularity_single_use
        Default: no_container

    scheduler_mode : str
        Select the mode of how the container is managed from hard, soft
        Default: hard

    container_type : str
        Select the type of container from Docker, Singularity, Shifter
        Default: None

    scaling_enabled : Bool
        Allow Interchange to manage resource provisioning. If set to False, interchange
        will not do any scaling.
        Default: True
    """

    def __init__(self,

                 # Execution backed
                 executors: list = [HighThroughputExecutor()],

                 # Scaling mechanics
                 provider=LocalProvider(),
                 scaling_enabled=True,
                 # Connection info
                 worker_ports=None,
                 worker_port_range=(54000, 55000),
                 funcx_service_address='https://api.funcx.org/v1',

                 # Tuning info
                 worker_mode='no_container',
                 scheduler_mode='hard',
                 container_type=None,
                 prefetch_capacity=10,
                 heartbeat_period=30,
                 heartbeat_threshold=120,
                 poll_period=10,
                 # Logging info
                 working_dir=None,
                 log_max_bytes=256 * (10 ** 6),
                 log_backup_count=1,
                 worker_debug=False):

        # Execution backends
        self.executors = executors  # List of executors

        # Scaling mechanics
        self.provider = provider
        self.scaling_enabled = scaling_enabled

        # Connection info
        self.worker_ports = worker_ports
        self.worker_port_range = worker_port_range
        self.funcx_service_address = funcx_service_address

        # Tuning info
        self.worker_mode = worker_mode
        self.scheduler_mode = scheduler_mode
        self.container_type = container_type
        self.prefetch_capacity = prefetch_capacity
        self.heartbeat_period = heartbeat_period
        self.heartbeat_threshold = heartbeat_threshold
        self.poll_period = poll_period

        # Logging info
        self.working_dir = working_dir
        self.worker_debug = worker_debug
        self.log_max_bytes = log_max_bytes
        self.log_backup_count = log_backup_count
