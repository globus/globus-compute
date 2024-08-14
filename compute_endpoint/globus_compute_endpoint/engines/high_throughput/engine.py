"""HighThroughputEngine builds on Parsl's HighThroughputExecutor for execution
of functions within containerized workers in a distributed setting.
"""

from __future__ import annotations

import concurrent.futures
import ipaddress
import logging
import multiprocessing
import os
import queue
import threading
import time
import typing as t
import uuid
import warnings
from concurrent.futures import Future
from multiprocessing import Process

import dill
from globus_compute_common import messagepack
from globus_compute_common.messagepack.message_types import (
    EPStatusReport as CommonEPStatusReport,
)
from globus_compute_common.messagepack.message_types import Result, ResultErrorDetails
from globus_compute_endpoint.endpoint.messages_compat import convert_ep_status_report
from globus_compute_endpoint.engines.base import GlobusComputeEngineBase
from globus_compute_endpoint.engines.helper import execute_task
from globus_compute_endpoint.engines.high_throughput import interchange, zmq_pipes
from globus_compute_endpoint.engines.high_throughput.mac_safe_queue import mpQueue
from globus_compute_endpoint.engines.high_throughput.messages import (
    EPStatusReport,
    Heartbeat,
    HeartbeatReq,
    Task,
    TaskCancel,
)
from globus_compute_endpoint.exception_handling import get_result_error_details
from globus_compute_endpoint.strategies.simple import SimpleStrategy
from globus_compute_sdk.serialize import ComputeSerializer
from parsl.errors import ConfigurationError
from parsl.executors.errors import BadMessage, ScalingFailed
from parsl.providers import LocalProvider
from parsl.utils import RepresentationMixin

serializer = ComputeSerializer()

log = logging.getLogger(__name__)


BUFFER_THRESHOLD = 1024 * 1024
ITEM_THRESHOLD = 1024


class HighThroughputEngine(GlobusComputeEngineBase, RepresentationMixin):
    """Engine designed for cluster-scale

    The HighThroughputEngine system has the following components:
      1. The HighThroughputEngine instance which is run as a client
      2. The Interchange which is acts as a load-balancing proxy between workers and
         Parsl
      3. The multiprocessing based worker pool which coordinates task execution over
         several cores on a node.
      4. ZeroMQ pipes connect the HighThroughputEngine, Interchange and the
         process_worker_pool

    Here is a diagram

    .. code:: python


                        |  Data   |  Engine     |  Interchange  | External Process(es)
                        |  Flow   |             |               |
                   Task | Kernel  |             |               |
                 +----->|-------->|------------>|->outgoing_q---|-> process_worker_pool
                 |      |         |             | batching      |    |         |
           Parsl<---Fut-|         |             | load-balancing|  result   exception
                     ^  |         |             | watchdogs     |    |         |
                     |  |         |   Q_mngmnt  |               |    V         V
                     |  |         |    Thread<--|-incoming_q<---|--- +---------+
                     |  |         |      |      |               |
                     |  |         |      |      |               |
                     +----update_fut-----+


    Parameters
    ----------

    provider : :class:`~parsl.providers.base.ExecutionProvider`
       Provider to access computation resources. Can be one of
       :class:`~parsl.providers.aws.aws.EC2Provider`,
        :class:`~parsl.providers.cobalt.cobalt.Cobalt`,
        :class:`~parsl.providers.condor.condor.Condor`,
        :class:`~parsl.providers.googlecloud.googlecloud.GoogleCloud`,
        :class:`~parsl.providers.gridEngine.gridEngine.GridEngine`,
        :class:`~parsl.providers.jetstream.jetstream.Jetstream`,
        :class:`~parsl.providers.local.local.Local`,
        :class:`~parsl.providers.sge.sge.GridEngine`,
        :class:`~parsl.providers.slurm.slurm.Slurm`, or
        :class:`~parsl.providers.torque.torque.Torque`.

    label : str
        Label for this Engine instance.

    launch_cmd : str
        Command line string to launch the process_worker_pool from the provider. The
        command line string will be formatted with appropriate values for the following
        values: (
            debug,
            task_url,
            result_url,
            cores_per_worker,
            nodes_per_block,
            heartbeat_period,
            heartbeat_threshold,
            logdir,
        ).
        For example:
        launch_cmd="process_worker_pool.py {debug} -c {cores_per_worker} \
        --task_url={task_url} --result_url={result_url}"

    address : string
        An address of the host on which the engine runs, which is reachable from the
        network in which workers will be running. This can be either a hostname as
        returned by `hostname` or an IP address. Most login nodes on clusters have
        several network interfaces available, only some of which can be reached
        from the compute nodes. Some trial and error might be necessary to
        indentify what addresses are reachable from compute nodes.

    worker_ports : (int, int)
        Specify the ports to be used by workers to connect to Parsl. If this
        option is specified, worker_port_range will not be honored.

    worker_port_range : (int, int)
        Worker ports will be chosen between the two integers provided.

    interchange_port_range : (int, int)
        Port range used by Parsl to communicate with the Interchange.

    working_dir : str
        Working dir to be used by the engine.

    worker_debug : Bool
        Enables worker debug logging.

    cores_per_worker : float
        cores to be assigned to each worker. Oversubscription is possible
        by setting cores_per_worker < 1.0. Default=1

    mem_per_worker : float
        Memory to be assigned to each worker. Default=None(no limits)

    available_accelerators: int, list of str
        Either a list of accelerators device IDs
        or an integer defining the number of accelerators available.
        If an integer, sequential device IDs will be created starting at 0.
        The manager will ensure each worker is pinned to an accelerator and
        will set the maximum number of workers per node to be no more
        than the number of accelerators.

        Workers are pinned to specific accelerators using environment variables,
        such as by setting the ``CUDA_VISIBLE_DEVICES`` or
        ``SYCL_DEVICE_FILTER`` to the selected accelerator.
        Default: None

    max_workers_per_node : int
        Caps the number of workers launched by the manager. Default: infinity

    suppress_failure : Bool
        If set, the interchange will suppress failures rather than terminate early.
        Default: False

    heartbeat_threshold : int
        Seconds since the last message from the counterpart in the communication pair:
        (interchange, manager) after which the counterpart is assumed to be unavailable.
        Default:120s

    heartbeat_period : int
        Number of seconds after which a heartbeat message indicating liveness is sent to
        the endpoint
        counterpart (interchange, manager). Default:30s

    poll_period : int
        Timeout period to be used by the engine components in milliseconds.
        Increasing poll_periods trades performance for cpu efficiency. Default: 10ms

    container_image : str
        Path or identfier to the container image to be used by the workers

    scheduler_mode: str
        Scheduling mode to be used by the node manager. Options: 'hard', 'soft'
        'hard' -> managers cannot replace worker's container types
        'soft' -> managers can replace unused worker's containers based on demand

    worker_mode : str
        Select the mode of operation from no_container, singularity_reuse,
        singularity_single_use
        Default: singularity_reuse

    container_cmd_options: str
        Container command strings to be added to associated container command.
        For example, singularity exec {container_cmd_options}

    task_status_queue : queue.Queue
        Queue to pass updates to task statuses back to the forwarder.

    strategy: Stategy Object
        Specify the scaling strategy to use for this engine.

    launch_cmd: str
        Specify the launch command as using f-string format that will be used to specify
        command to launch managers. Default: None

    prefetch_capacity: int
        Number of tasks that can be fetched by managers in excess of available
        workers is a prefetching optimization. This option can cause poor
        load-balancing for long running functions.
        Default: 10

    provider: Provider object
        Provider determines how managers can be provisioned, say LocalProvider
        offers forked processes, and SlurmProvider interfaces to request
        resources from the Slurm batch scheduler.
        Default: LocalProvider
    """

    def __init__(
        self,
        label="HighThroughputEngine",
        # NEW
        strategy=SimpleStrategy(),
        max_workers_per_node=float("inf"),
        mem_per_worker=None,
        launch_cmd=None,
        available_accelerators=None,
        # Container specific
        worker_mode="no_container",
        scheduler_mode="hard",
        container_type=None,
        container_cmd_options="",
        cold_routing_interval=10.0,
        # Tuning info
        prefetch_capacity=10,
        provider=LocalProvider(),
        address="127.0.0.1",
        worker_ports=None,
        worker_port_range=(54000, 55000),
        interchange_port_range=(55000, 56000),
        storage_access=None,
        working_dir=None,
        worker_debug=False,
        cores_per_worker=1.0,
        heartbeat_threshold=120,
        heartbeat_period=30,
        poll_period=10,
        container_image=None,
        suppress_failure=True,
        run_dir=None,
        endpoint_id=None,
        passthrough=True,
        task_status_queue=None,
    ):
        warnings.warn(
            "HighThroughputEngine is deprecated."
            " Please use GlobusComputeEngine instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        log.debug("Initializing HighThroughputEngine")
        self.provider = provider
        self.label = label
        self.launch_cmd = launch_cmd
        self.worker_debug = worker_debug
        self.max_workers_per_node = max_workers_per_node

        # NEW
        self.strategy = strategy
        self.cores_per_worker = cores_per_worker
        self.mem_per_worker = mem_per_worker

        # Container specific
        self.scheduler_mode = scheduler_mode
        self.container_type = container_type
        self.container_cmd_options = container_cmd_options
        self.cold_routing_interval = cold_routing_interval

        # Tuning info
        self.prefetch_capacity = prefetch_capacity

        self.storage_access = storage_access if storage_access is not None else []
        if len(self.storage_access) > 1:
            raise ConfigurationError(
                "Multiple storage access schemes are not supported"
            )
        self.working_dir = working_dir
        self.blocks = []
        self.cores_per_worker = cores_per_worker
        self.endpoint_id = endpoint_id
        self._task_counter = 0

        try:
            ipaddress.ip_address(address=address)
        except Exception:
            log.critical(
                f"Invalid address supplied: {address}. "
                "Please use a valid IPv4 or IPv6 address"
            )
            raise
        self.address = address
        self.worker_ports = worker_ports
        self.worker_port_range = worker_port_range
        self.interchange_port_range = interchange_port_range
        self.heartbeat_threshold = heartbeat_threshold
        self.heartbeat_period = heartbeat_period
        self.poll_period = poll_period
        self.suppress_failure = suppress_failure
        self.run_dir = run_dir
        self.queue_proc: multiprocessing.Process | None = None
        self.passthrough = passthrough
        self.task_status_queue = task_status_queue
        self.tasks = {}

        self.outgoing_q: zmq_pipes.TasksOutgoing | None = None
        self.incoming_q: zmq_pipes.ResultsIncoming | None = None
        self.command_client: zmq_pipes.CommandClient | None = None
        self.results_passthrough: queue.Queue | None = None
        self._queue_management_thread: threading.Thread | None = None

        self.is_alive = False

        # Set the available accelerators
        if available_accelerators is None:
            self.available_accelerators = ()
        else:
            if isinstance(available_accelerators, int):
                self.available_accelerators = [
                    str(i) for i in range(available_accelerators)
                ]
            else:
                self.available_accelerators = list(available_accelerators)
            log.debug(
                "Workers will be assigned "
                f"to accelerators: {self.available_accelerators}"
            )

        # Globus Compute specific options
        self.container_image = container_image
        self.worker_mode = worker_mode

        if not launch_cmd:
            self.launch_cmd = (
                "process_worker_pool.py {debug} {max_workers} "
                "-c {cores_per_worker} "
                "--poll {poll_period} "
                "--task_url={task_url} "
                "--result_url={result_url} "
                "--logdir={logdir} "
                "--hb_period={heartbeat_period} "
                "--hb_threshold={heartbeat_threshold} "
                "--mode={worker_mode} "
                "--container_image={container_image} "
            )

    def start(
        self,
        *args,
        endpoint_id: uuid.UUID | None = None,
        run_dir: str | None = None,
        results_passthrough: queue.Queue[dict[str, bytes | str | None]] | None = None,
        **kwargs,
    ):
        """Create the Interchange process and connect to it."""
        assert run_dir, "HighThroughputEngine requires kwarg:run_dir at start"
        assert endpoint_id, "HighThroughputEngine requires kwarg:endpoint_id at start"
        self.run_dir = run_dir
        self.endpoint_id = endpoint_id

        self.outgoing_q = zmq_pipes.TasksOutgoing(
            "127.0.0.1", self.interchange_port_range
        )
        self.incoming_q = zmq_pipes.ResultsIncoming(
            "127.0.0.1", self.interchange_port_range
        )
        self.command_client = zmq_pipes.CommandClient(
            "127.0.0.1", self.interchange_port_range
        )

        self.is_alive = True

        if self.passthrough is True:
            if results_passthrough is None:
                raise Exception(
                    "Engines configured in passthrough mode, must be started with"
                    "a multiprocessing queue for results_passthrough"
                )
            self.results_passthrough = results_passthrough
            log.debug(f"Engine:{self.label} starting in results_passthrough mode")

        self._engine_bad_state = threading.Event()
        self._engine_exception: t.Optional[Exception] = None
        self._start_queue_management_thread()

        log.info("Attempting local interchange start")
        self._start_local_interchange_process()
        log.info(
            "Started local interchange with ports: %s. %s",
            self.worker_task_port,
            self.worker_result_port,
        )

        log.debug(f"Created management thread: {self._queue_management_thread}")

        if self.provider:
            pass
        else:
            self._scaling_enabled = False
            log.debug("Starting HighThroughputEngine with no provider")

        return self.outgoing_q.port, self.incoming_q.port, self.command_client.port

    def _start_local_interchange_process(self):
        """Starts the interchange process locally

        Starts the interchange process locally and uses an internal command queue to
        get the worker task and result ports that the interchange has bound to.
        """
        comm_q = mpQueue(maxsize=10)
        self.queue_proc = Process(
            target=interchange.starter,
            name="Engine-Interchange",
            args=(comm_q,),
            kwargs={
                "client_address": "127.0.0.1",  # engine and ix are on the same node
                "client_ports": (
                    self.outgoing_q.port,
                    self.incoming_q.port,
                    self.command_client.port,
                ),
                "provider": self.provider,
                "strategy": self.strategy,
                "poll_period": self.poll_period,
                "heartbeat_period": self.heartbeat_period,
                "heartbeat_threshold": self.heartbeat_threshold,
                "working_dir": self.working_dir,
                "worker_debug": self.worker_debug,
                "max_workers_per_node": self.max_workers_per_node,
                "mem_per_worker": self.mem_per_worker,
                "cores_per_worker": self.cores_per_worker,
                "available_accelerators": self.available_accelerators,
                "prefetch_capacity": self.prefetch_capacity,
                "scheduler_mode": self.scheduler_mode,
                "worker_mode": self.worker_mode,
                "container_type": self.container_type,
                "container_cmd_options": self.container_cmd_options,
                "cold_routing_interval": self.cold_routing_interval,
                "interchange_address": self.address,
                "worker_ports": self.worker_ports,
                "worker_port_range": self.worker_port_range,
                "logdir": os.path.join(self.run_dir, self.label),
                "suppress_failure": self.suppress_failure,
                "endpoint_id": self.endpoint_id,
            },
        )
        self.queue_proc.start()
        msg = None
        try:
            msg = comm_q.get(block=True, timeout=120)
        except queue.Empty:
            log.error("Interchange did not complete initialization.")

        if not msg:
            # poor-person's attempt to not interweave the traceback log lines
            # with the subprocess' likely traceback lines
            time.sleep(0.5)
            raise Exception("Interchange failed to start")

        comm_q.close()  # not strictly necessary, but be plain about intentions
        comm_q.join_thread()
        del comm_q

        self.worker_task_port, self.worker_result_port = msg

        self.worker_task_url = f"tcp://{self.address}:{self.worker_task_port}"
        self.worker_result_url = "tcp://{}:{}".format(
            self.address, self.worker_result_port
        )

    def get_status_report(self) -> CommonEPStatusReport:
        """HTEX Interchange reports EPStatusReport periodically"""
        raise NotImplementedError

    def _queue_management_worker(self):
        """Listen to the queue for task status messages and handle them.

        Depending on the message, tasks will be updated with results, exceptions,
        or updates. It expects the following messages:

        .. code:: python

            {
               "task_id" : <task_id>
               "result"  : serialized result object, if task succeeded
               ... more tags could be added later
            }

            {
               "task_id" : <task_id>
               "exception" : serialized exception object, on failure
            }

        We do not support these yet, but they could be added easily.

        .. code:: python

            {
               "task_id" : <task_id>
               "cpu_stat" : <>
               "mem_stat" : <>
               "io_stat"  : <>
               "started"  : tstamp
            }

        The `None` message is a die request.
        """
        log.debug("queue management worker starting")

        while self.is_alive and not self._engine_bad_state.is_set():
            try:
                msgs = self.incoming_q.get(timeout=1)

            except queue.Empty:
                log.debug("queue empty")
                # Timed out.
                continue

            except OSError as e:
                log.exception(f"Caught broken queue with exception code {e.errno}: {e}")
                return

            except Exception as e:
                log.exception(f"Caught unknown exception: {e}")
                return

            else:
                if msgs is None:
                    log.debug("Got None, exiting")
                    return

                elif isinstance(msgs, EPStatusReport):
                    log.debug(f"Received {msgs!r}")
                    if self.passthrough:
                        external_ep_status = convert_ep_status_report(msgs)
                        self.results_passthrough.put(
                            {"message": messagepack.pack(external_ep_status)}
                        )

                else:
                    log.debug("Unpacking results")
                    for serialized_msg in msgs:
                        try:
                            msg = dill.loads(serialized_msg)
                            tid = msg["task_id"]
                        except dill.UnpicklingError:
                            raise BadMessage("Message received could not be unpickled")

                        except Exception:
                            raise BadMessage(
                                "Message received does not contain 'task_id' field"
                            )

                        if tid == -1 and "exception" in msg:
                            # TODO: This could be handled better we are
                            # essentially shutting down the client with little
                            # indication to the user.
                            log.warning(
                                "Engine shutting down due to fatal "
                                "exception from interchange"
                            )
                            self._engine_exception = serializer.deserialize(
                                msg["exception"]
                            )
                            log.exception(f"Exception: {self._engine_exception}")
                            # Set bad state to prevent new tasks from being submitted
                            self._engine_bad_state.set()
                            # We set all current tasks to this exception to make sure
                            # that this is raised in the main context.
                            # YADU: Report failure on all pending tasks
                            for task_id in self.tasks:
                                try:
                                    self.tasks[task_id].set_exception(
                                        self._engine_exception
                                    )
                                except concurrent.futures.InvalidStateError:
                                    # Task was already cancelled, the exception can be
                                    # ignored
                                    log.debug(
                                        f"Task:{task_id} result couldn't be set. "
                                        "Already in terminal state"
                                    )
                            break

                        if self.passthrough is True:
                            log.debug(f"Pushing results for task:{tid}")
                            # we are only interested in actual task ids here, not
                            # identifiers for other message types
                            sent_task_id = tid if isinstance(tid, str) else None
                            packed_result = self._convert_result_to_outgoing(
                                sent_task_id, msg
                            )
                            if packed_result is None:
                                continue
                            task_result = {
                                "task_id": sent_task_id,
                                "message": packed_result,
                            }
                            self.results_passthrough.put(task_result)
                            self.tasks[sent_task_id].set_result(packed_result)
                            continue

                        try:
                            task_fut = self.tasks.pop(tid)
                        except KeyError:
                            # This is triggered when the result of a cancelled task is
                            # returned
                            # We should log, and proceed.
                            log.warning(
                                f"Task:{tid} not found in tasks table\n"
                                "Task likely was cancelled and removed."
                            )
                            continue

                        if "result" in msg:
                            result = serializer.deserialize(msg["result"])
                            try:
                                task_fut.set_result(result)
                            except concurrent.futures.InvalidStateError:
                                log.debug(
                                    f"Task:{tid} result couldn't be set. "
                                    "Already in terminal state"
                                )
                        elif "exception" in msg:
                            exception = serializer.deserialize(msg["exception"])
                            try:
                                task_fut.set_result(exception)
                            except concurrent.futures.InvalidStateError:
                                log.debug(
                                    f"Task:{tid} result couldn't be set. "
                                    "Already in terminal state"
                                )
                        else:
                            raise BadMessage(
                                "Message received is neither result or exception"
                            )

        log.info("queue management worker finished")

    def _convert_result_to_outgoing(
        self, task_id: str | None, msg: dict[str, t.Union[str, bytes]]
    ) -> bytes | None:
        """The messagepacked result should be in the result dict in the data field"""
        try:
            # The data body already is packed on the worker
            assert isinstance(msg["data"], bytes)
            return msg["data"]
        except (KeyError, AssertionError) as e:
            log.debug(f"Invalid result message: {msg}", exc_info=e)
            if task_id is None:
                return None
            err_code, err_msg = get_result_error_details()
            failed_result = Result(
                task_id=task_id,
                data=f"Task {task_id} failed to run on endpoint {self.endpoint_id}",
                error_details=ResultErrorDetails(code=err_code, user_message=err_msg),
            )
            return messagepack.pack(failed_result)

    # When the engine gets lost, the weakref callback will wake up
    # the queue management thread.
    def weakref_cb(self, q=None):
        """We do not use this yet."""
        q.put(None)

    def _start_queue_management_thread(self):
        """Method to start the management thread as a daemon.

        Checks if a thread already exists, then starts it.
        Could be used later as a restart if the management thread dies.
        """
        if self._queue_management_thread is None:
            log.debug("Starting queue management thread")
            self._queue_management_thread = threading.Thread(
                target=self._queue_management_worker, name="Queue-Management"
            )
            self._queue_management_thread.daemon = True
            self._queue_management_thread.start()
            log.debug("Started queue management thread")

        else:
            log.debug("Management thread already exists, returning")

    def hold_worker(self, worker_id):
        """Puts a worker on hold, preventing scheduling of additional tasks to it.

        This is called "hold" mostly because this only stops scheduling of tasks,
        and does not actually kill the worker.

        Parameters
        ----------

        worker_id : str
            Worker id to be put on hold
        """
        c = self.command_client.run(f"HOLD_WORKER;{worker_id}")
        log.debug(f"Sent hold request to worker: {worker_id}")
        return c

    def send_heartbeat(self):
        log.warning("Sending heartbeat to interchange")
        msg = Heartbeat(endpoint_id="")
        self.outgoing_q.put(msg.pack())

    def wait_for_endpoint(self):
        heartbeat = self.command_client.run(HeartbeatReq())
        log.debug("Attempting heartbeat to interchange")
        return heartbeat

    @property
    def outstanding(self):
        outstanding_c = self.command_client.run("OUTSTANDING_C")
        log.debug(f"Got outstanding count: {outstanding_c}")
        return outstanding_c

    @property
    def connected_workers(self):
        workers = self.command_client.run("MANAGERS")
        log.debug(f"Got managers: {workers}")
        return workers

    def _submit(self, func: t.Callable, *args: t.Any, **kwargs: t.Any) -> Future:
        raise RuntimeError("Invalid attempt to _submit()")

    def _get_container_location(self, packed_task: bytes) -> str:
        """Unpack the task message to get the container location."""
        task_msg = messagepack.unpack(packed_task)
        if not isinstance(task_msg, messagepack.message_types.Task):
            raise messagepack.InvalidMessageError()

        container_loc = "RAW"
        if task_msg.container:
            for img in task_msg.container.images:
                if img.image_type == self.container_type:
                    container_loc = img.location
                    break

        return container_loc

    def submit(
        self, task_id: str, packed_task: bytes, resource_specification: t.Dict
    ) -> HTEXFuture:
        """Submits a messagepacked.Task for execution

        Parameters
        ----------
        Packed Task (messages.Task) - A packed Task object which contains task_id,
        container_id, and serialized fn, args, kwargs packages.

        Returns:
              Submit status
        """
        if self._engine_bad_state.is_set():
            # If the flag is set the exception body must exist
            raise self._engine_exception  # type: ignore

        self._task_counter += 1

        future = HTEXFuture(self, task_id)
        self.tasks[task_id] = future

        container_loc = self._get_container_location(packed_task)
        ser = serializer.serialize(
            (execute_task, [task_id, packed_task, self.endpoint_id], {})
        )
        payload = Task(task_id, container_loc, ser).pack()
        assert self.outgoing_q  # Placate mypy
        self.outgoing_q.put(payload)

        return future

    def _get_block_and_job_ids(self):
        # Not using self.blocks.keys() and self.blocks.values() simultaneously
        # The dictionary may be changed during invoking this function
        # As scale_in and scale_out are invoked in multiple threads
        block_ids = list(self.blocks.keys())
        job_ids = []  # types: List[Any]
        for bid in block_ids:
            job_ids.append(self.blocks[bid])
        return block_ids, job_ids

    @property
    def connection_info(self):
        """All connection info necessary for the endpoint to connect back

        Returns:
              Dict with connection info
        """
        return {
            "address": self.address,
            # A memorial to the ungodly amount of time and effort spent,
            # troubleshooting the order of these ports.
            "client_ports": "{},{},{}".format(
                self.outgoing_q.port, self.incoming_q.port, self.command_client.port
            ),
        }

    @property
    def scaling_enabled(self):
        return self._scaling_enabled

    def scale_out(self, blocks=1):
        """Scales out the number of blocks by "blocks"

        Raises:
             NotImplementedError
        """
        r = []
        for i in range(blocks):
            if self.provider:
                block = self.provider.submit(self.launch_cmd, 1, 1)
                log.debug(f"Launched block {i}:{block}")
                if not block:
                    raise (
                        ScalingFailed(
                            self.provider.label,
                            "Attempts to provision nodes via provider has failed",
                        )
                    )
                self.blocks.extend([block])
            else:
                log.error("No execution provider available")
                r = None
        return r

    def scale_in(self, blocks):
        """Scale in the number of active blocks by specified amount.

        The scale in method here is very rude. It doesn't give the workers
        the opportunity to finish current tasks or cleanup. This is tracked
        in issue #530

        Raises:
             NotImplementedError
        """
        to_kill = self.blocks[:blocks]
        if self.provider:
            r = self.provider.cancel(to_kill)
        return r

    def _get_job_ids(self):
        return list(self.block.values())

    def status(self):
        """Return status of all blocks."""

        status = []
        if self.provider:
            status = self.provider.status(self.blocks)

        return status

    def shutdown(self, /, hub=True, targets="all", block=False, **kwargs) -> None:
        """Shutdown the Engine, including all workers and controllers.

        This is not implemented.

        Kwargs:
            - hub (Bool): Whether the hub should be shutdown, Default:True,
            - targets (list of ints| 'all'): List of block id's to kill, Default:'all'
            - block (Bool): To block for confirmations or not

        Raises:
             NotImplementedError
        """

        log.info("Attempting HighThroughputEngine shutdown")
        if self.queue_proc:
            try:
                self.queue_proc.terminate()
            except AttributeError:
                log.info("Engine interchange terminate skipped due to wrong context")
            except Exception:
                log.exception("Terminating the interchange failed")
            if block:
                self.queue_proc.join(timeout=5)
                if self.queue_proc.exitcode is None:
                    self.queue_proc.kill()
            log.info("Finished HighThroughputEngine shutdown attempt")

    def cancel_task(self, task_id: uuid.UUID | str) -> bool:
        """
        Attempt to cancel the task `task_id` by requesting cancellation
        from the interchange.  Task cancellation is attempted only if the
        future is cancellable (i.e., not already in a terminal state).  This
        relies on the Engine not setting the task to a running state, and the
        task only tracking pending, and completed states.

        Parameters
        ----------
        task_id

        Returns
        -------
        Bool
        """

        log.debug("Send TaskCancel to interchange (%s)", task_id)
        log.debug("Sending cancel of task_id:{future.task_id} to interchange")
        # TODO: Doesn't yet return a bool ...
        assert self.command_client
        return self.command_client.run(TaskCancel(str(task_id)))


CANCELLED = "CANCELLED"
CANCELLED_AND_NOTIFIED = "CANCELLED_AND_NOTIFIED"
FINISHED = "FINISHED"


class HTEXFuture(concurrent.futures.Future):
    __slots__ = ("engine", "task_id")

    def __init__(self, engine: HighThroughputEngine, task_id: str):
        super().__init__()
        self.engine = engine
        self.task_id = task_id

    def cancel(self):
        raise NotImplementedError(
            f"{self.__class__} does not implement cancel() "
            "try using best_effort_cancel()"
        )

    def best_effort_cancel(self):
        """Attempt to cancel the function.

        If the function has finished running, the task cannot be cancelled
        and the method will return False.
        If the function is yet to start or is running, cancellation will be
        attempted without guarantees, and the method will return True.

        Please note that a return value of True does not guarantee that your
        function will not execute at all, but it does guarantee that the
        future will be in a cancelled state.

        Returns
        -------
        Bool
        """
        return self.engine.cancel_task(self.task_id)
