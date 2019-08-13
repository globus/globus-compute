#!/usr/bin/env python
import argparse
import zmq
# import uuid
import os
import sys
import platform
import random
import time
import pickle
import logging
import queue
import threading
import json
import daemon

from parsl.version import VERSION as PARSL_VERSION
from ipyparallel.serialize import serialize_object

LOOP_SLOWDOWN = 0.0  # in seconds
HEARTBEAT_CODE = (2 ** 32) - 1
PKL_HEARTBEAT_CODE = pickle.dumps((2 ** 32) - 1)


class ShutdownRequest(Exception):
    ''' Exception raised when any async component receives a ShutdownRequest
    '''
    def __init__(self):
        self.tstamp = time.time()

    def __repr__(self):
        return "Shutdown request received at {}".format(self.tstamp)


class ManagerLost(Exception):
    ''' Task lost due to worker loss. Worker is considered lost when multiple heartbeats
    have been missed.
    '''
    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.tstamp = time.time()

    def __repr__(self):
        return "Task failure due to loss of worker {}".format(self.worker_id)


class BadRegistration(Exception):
    ''' A new Manager tried to join the executor with a BadRegistration message
    '''
    def __init__(self, worker_id, critical=False):
        self.worker_id = worker_id
        self.tstamp = time.time()
        self.handled = "critical" if critical else "suppressed"

    def __repr__(self):
        return "Manager:{} caused a {} failure".format(self.worker_id,
                                                       self.handled)


class Interchange(object):
    """ Interchange is a task orchestrator for distributed systems.

    1. Asynchronously queue large volume of tasks (>100K)
    2. Allow for workers to join and leave the union
    3. Detect workers that have failed using heartbeats
    4. Service single and batch requests from workers
    5. Be aware of requests worker resource capacity,
       eg. schedule only jobs that fit into walltime.

    TODO: We most likely need a PUB channel to send out global commands, like shutdown
    """
    def __init__(self,
                 config,
                 client_address="127.0.0.1",
                 interchange_address="127.0.0.1",
                 client_ports=(50055, 50056, 50057),
                 worker_ports=None,
                 worker_port_range=(54000, 55000),
                 cores_per_worker=1.0,
                 worker_debug=False,
                 launch_cmd=None,
                 heartbeat_threshold=60,
                 logdir=".",
                 logging_level=logging.INFO,
                 poll_period=10,
                 endpoint_id=None,
                 suppress_failure=False,
             ):
        """
        Parameters
        ----------
        config : funcx.Config object
             Funcx config object that describes how compute should be provisioned

        client_address : str
             The ip address at which the parsl client can be reached. Default: "127.0.0.1"

        interchange_address : str
             The ip address at which the workers will be able to reach the Interchange. Default: "127.0.0.1"

        client_ports : triple(int, int, int)
             The ports at which the client can be reached

        launch_cmd : str
             TODO : update

        worker_ports : tuple(int, int)
             The specific two ports at which workers will connect to the Interchange. Default: None

        worker_port_range : tuple(int, int)
             The interchange picks ports at random from the range which will be used by workers.
             This is overridden when the worker_ports option is set. Defauls: (54000, 55000)

        cores_per_worker : float
             cores to be assigned to each worker. Oversubscription is possible
             by setting cores_per_worker < 1.0. Default=1

        worker_debug : Bool
             Enables worker debug logging.

        heartbeat_threshold : int
             Number of seconds since the last heartbeat after which worker is considered lost.

        logdir : str
             Parsl log directory paths. Logs and temp files go here. Default: '.'

        logging_level : int
             Logging level as defined in the logging module. Default: logging.INFO (20)

        endpoint_id : str
             Identity string that identifies the endpoint to the broker

        poll_period : int
             The main thread polling period, in milliseconds. Default: 10ms

        suppress_failure : Bool
             When set to True, the interchange will attempt to suppress failures. Default: False

        """
        self.logdir = logdir
        try:
            os.makedirs(self.logdir)
        except FileExistsError:
            pass

        start_file_logger("{}/interchange.log".format(self.logdir), level=logging_level)
        logger.info("Initializing Interchange process with Endpoint ID: {}".format(endpoint_id))
        self.config = config
        logger.info("Got config : {}".format(config))

        self.client_address = client_address
        self.interchange_address = interchange_address
        self.suppress_failure = suppress_failure
        self.poll_period = poll_period

        logger.info("Attempting connection to client at {} on ports: {},{},{}".format(
            client_address, client_ports[0], client_ports[1], client_ports[2]))
        self.context = zmq.Context()
        self.task_incoming = self.context.socket(zmq.DEALER)
        self.task_incoming.set_hwm(0)
        self.task_incoming.RCVTIMEO = 10  # in milliseconds
        logger.info("Task incoming on tcp://{}:{}".format(client_address, client_ports[0]))
        self.task_incoming.connect("tcp://{}:{}".format(client_address, client_ports[0]))

        self.results_outgoing = self.context.socket(zmq.DEALER)
        self.results_outgoing.set_hwm(0)
        logger.info("Results outgoing on tcp://{}:{}".format(client_address, client_ports[1]))
        self.results_outgoing.connect("tcp://{}:{}".format(client_address, client_ports[1]))

        self.command_channel = self.context.socket(zmq.REP)
        self.command_channel.RCVTIMEO = 1000  # in milliseconds
        logger.info("Command channel on tcp://{}:{}".format(client_address, client_ports[2]))
        self.command_channel.connect("tcp://{}:{}".format(client_address, client_ports[2]))
        logger.info("Connected to client")

        self.pending_task_queue = queue.Queue(maxsize=10 ** 6)

        self.worker_ports = worker_ports
        self.worker_port_range = worker_port_range

        self.task_outgoing = self.context.socket(zmq.ROUTER)
        self.task_outgoing.set_hwm(0)
        self.results_incoming = self.context.socket(zmq.ROUTER)
        self.results_incoming.set_hwm(0)

        self.endpoint_id = endpoint_id
        if self.worker_ports:
            self.worker_task_port = self.worker_ports[0]
            self.worker_result_port = self.worker_ports[1]

            self.task_outgoing.bind("tcp://*:{}".format(self.worker_task_port))
            self.results_incoming.bind("tcp://*:{}".format(self.worker_result_port))

        else:
            self.worker_task_port = self.task_outgoing.bind_to_random_port('tcp://*',
                                                                           min_port=worker_port_range[0],
                                                                           max_port=worker_port_range[1], max_tries=100)
            self.worker_result_port = self.results_incoming.bind_to_random_port('tcp://*',
                                                                                min_port=worker_port_range[0],
                                                                                max_port=worker_port_range[1], max_tries=100)

        logger.info("Bound to ports {},{} for incoming worker connections".format(
            self.worker_task_port, self.worker_result_port))

        self._ready_manager_queue = {}

        self.heartbeat_threshold = heartbeat_threshold

        self.launch_cmd = launch_cmd
        if not launch_cmd:
            self.launch_cmd = ("funcx-manager {debug} {max_workers} "
                               "-c {cores_per_worker} "
                               "--poll {poll_period} "
                               "--task_url={task_url} "
                               "--result_url={result_url} "
                               "--logdir={logdir} "
                               "--hb_period={heartbeat_period} "
                               "--hb_threshold={heartbeat_threshold} "
                               "--mode={worker_mode} "
                               "--container_image={container_image} ")

        self.current_platform = {'parsl_v': PARSL_VERSION,
                                 'python_v': "{}.{}.{}".format(sys.version_info.major,
                                                               sys.version_info.minor,
                                                               sys.version_info.micro),
                                 'os': platform.system(),
                                 'hname': platform.node(),
                                 'dir': os.getcwd()}

        logger.info("Platform info: {}".format(self.current_platform))
        try:
            self.load_config()
        except Exception as e:
            logger.exception("Caught exception")
            raise


    def load_config(self):
        """ Load the config
        """
        logger.info("Loading endpoint local config")
        working_dir = self.config.working_dir
        if self.config.working_dir is None:
            working_dir = "{}/{}".format(self.logdir, "worker_logs")
        logger.info("Setting working_dir: {}".format(working_dir))

        self.config.provider.script_dir = working_dir
        self.config.provider.channel.script_dir = os.path.join(working_dir, 'submit_scripts')
        self.config.provider.channel.makedirs(self.config.provider.channel.script_dir, exist_ok=True)
        os.makedirs(self.config.provider.script_dir, exist_ok=True)

        debug_opts = "--debug" if self.config.worker_debug else ""
        max_workers = "" if self.config.max_workers_per_node == float('inf') \
                      else "--max_workers={}".format(self.config.max_workers_per_node)

        worker_task_url = f"tcp://127.0.0.1:{self.worker_task_port}"
        worker_result_url = f"tcp://127.0.0.1:{self.worker_result_port}"

        l_cmd = self.launch_cmd.format(debug=debug_opts,
                                       max_workers=max_workers,
                                       cores_per_worker=self.config.cores_per_worker,
                                       #mem_per_worker=self.config.mem_per_worker,
                                       prefetch_capacity=self.config.prefetch_capacity,
                                       task_url=worker_task_url,
                                       result_url=worker_result_url,
                                       nodes_per_block=self.config.provider.nodes_per_block,
                                       heartbeat_period=self.config.heartbeat_period,
                                       heartbeat_threshold=self.config.heartbeat_threshold,
                                       poll_period=self.config.poll_period,
                                       worker_mode=self.config.worker_mode,
                                       container_image=None,
                                       logdir=working_dir)
        self.config.launch_cmd = l_cmd
        logger.info("Launch command: {}".format(self.config.launch_cmd))

        if self.config.scaling_enabled:
            logger.info("Scaling ...")
            self.config.provider.submit(self.config.launch_cmd, 1, 1)


    def get_tasks(self, count):
        """ Obtains a batch of tasks from the internal pending_task_queue

        Parameters
        ----------
        count: int
            Count of tasks to get from the queue

        Returns
        -------
        List of upto count tasks. May return fewer than count down to an empty list
            eg. [{'task_id':<x>, 'buffer':<buf>} ... ]
        """
        tasks = []
        for i in range(0, count):
            try:
                x = self.pending_task_queue.get(block=False)
            except queue.Empty:
                break
            else:
                tasks.append(x)

        return tasks

    def migrate_tasks_to_internal(self, kill_event):
        """Pull tasks from the incoming tasks 0mq pipe onto the internal
        pending task queue

        Parameters:
        -----------
        kill_event : threading.Event
              Event to let the thread know when it is time to die.
        """
        logger.info("[TASK_PULL_THREAD] Starting")
        task_counter = 0
        poller = zmq.Poller()
        poller.register(self.task_incoming, zmq.POLLIN)

        while not kill_event.is_set():
            try:
                msg = self.task_incoming.recv_pyobj()
            except zmq.Again:
                # We just timed out while attempting to receive
                logger.debug("[TASK_PULL_THREAD] {} tasks in internal queue".format(self.pending_task_queue.qsize()))
                continue

            if msg == 'STOP':
                kill_event.set()
                break
            else:
                self.pending_task_queue.put(msg)
                task_counter += 1
                logger.debug("[TASK_PULL_THREAD] Fetched task:{}".format(task_counter))

    def _command_server(self, kill_event):
        """ Command server to run async command to the interchange
        """
        logger.debug("[COMMAND] Command Server Starting")
        while not kill_event.is_set():
            try:
                command_req = self.command_channel.recv_pyobj()
                logger.debug("[COMMAND] Received command request: {}".format(command_req))
                if command_req == "OUTSTANDING_C":
                    outstanding = self.pending_task_queue.qsize()
                    for manager in self._ready_manager_queue:
                        outstanding += len(self._ready_manager_queue[manager]['tasks'])
                    reply = outstanding

                elif command_req == "MANAGERS":
                    reply = []
                    for manager in self._ready_manager_queue:
                        resp = (manager.decode('utf-8'),
                                len(self._ready_manager_queue[manager]['tasks']),
                                self._ready_manager_queue[manager]['active'])
                        reply.append(resp)

                elif command_req.startswith("HOLD_WORKER"):
                    cmd, s_manager = command_req.split(';')
                    manager = s_manager.encode('utf-8')
                    logger.info("[CMD] Received HOLD_WORKER for {}".format(manager))
                    if manager in self._ready_manager_queue:
                        self._ready_manager_queue[manager]['active'] = False
                        reply = True
                    else:
                        reply = False

                elif command_req == "HEARTBEAT":
                    logger.info("[CMD] Received heartbeat message from hub")
                    reply = "HBT,{}".format(self.endpoint_id)

                elif command_req == "SHUTDOWN":
                    logger.info("[CMD] Received SHUTDOWN command")
                    kill_event.set()
                    reply = True

                else:
                    reply = None

                logger.debug("[COMMAND] Reply: {}".format(reply))
                self.command_channel.send_pyobj(reply)

            except zmq.Again:
                logger.debug("[COMMAND] is alive")
                continue

    def start(self, poll_period=None):
        """ Start the NeedNameQeueu

        Parameters:
        ----------

        TODO: Move task receiving to a thread
        """
        logger.info("Incoming ports bound")

        if poll_period is None:
            poll_period = self.poll_period

        start = time.time()
        count = 0

        self._kill_event = threading.Event()
        self._task_puller_thread = threading.Thread(target=self.migrate_tasks_to_internal,
                                                    args=(self._kill_event,))
        self._task_puller_thread.start()

        self._command_thread = threading.Thread(target=self._command_server,
                                                args=(self._kill_event,))
        self._command_thread.start()

        poller = zmq.Poller()
        # poller.register(self.task_incoming, zmq.POLLIN)
        poller.register(self.task_outgoing, zmq.POLLIN)
        poller.register(self.results_incoming, zmq.POLLIN)

        # These are managers which we should examine in an iteration
        # for scheduling a job (or maybe any other attention?).
        # Anything altering the state of the manager should add it
        # onto this list.
        interesting_managers = set()

        while not self._kill_event.is_set():
            self.socks = dict(poller.poll(timeout=poll_period))

            # Listen for requests for work
            if self.task_outgoing in self.socks and self.socks[self.task_outgoing] == zmq.POLLIN:
                logger.debug("[MAIN] starting task_outgoing section")
                message = self.task_outgoing.recv_multipart()
                manager = message[0]

                if manager not in self._ready_manager_queue:
                    reg_flag = False

                    try:
                        msg = json.loads(message[1].decode('utf-8'))
                        reg_flag = True
                    except Exception:
                        logger.warning("[MAIN] Got a non-json registration message from manager:{}".format(
                            manager))
                        logger.debug("[MAIN] Message :\n{}\n".format(message[0]))

                    # By default we set up to ignore bad nodes/registration messages.
                    self._ready_manager_queue[manager] = {'last': time.time(),
                                                          'free_capacity': 0,
                                                          'active': True,
                                                          'tasks': []}
                    if reg_flag is True:
                        interesting_managers.add(manager)
                        logger.info("[MAIN] Adding manager: {} to ready queue".format(manager))
                        self._ready_manager_queue[manager].update(msg)
                        logger.info("[MAIN] Registration info for manager {}: {}".format(manager, msg))

                        if (msg['python_v'] != self.current_platform['python_v'] or
                            msg['parsl_v'] != self.current_platform['parsl_v']):
                            logger.warn("[MAIN] Manager {} has incompatible version info with the interchange".format(manager))

                            if self.suppress_failure is False:
                                logger.debug("Setting kill event")
                                self._kill_event.set()
                                e = ManagerLost(manager)
                                result_package = {'task_id': -1, 'exception': serialize_object(e)}
                                pkl_package = pickle.dumps(result_package)
                                self.results_outgoing.send(pkl_package)
                                logger.warning("[MAIN] Sent failure reports, unregistering manager")
                            else:
                                logger.debug("[MAIN] Suppressing shutdown due to version incompatibility")

                    else:
                        # Registration has failed.
                        if self.suppress_failure is False:
                            self._kill_event.set()
                            e = BadRegistration(manager, critical=True)
                            result_package = {'task_id': -1, 'exception': serialize_object(e)}
                            pkl_package = pickle.dumps(result_package)
                            self.results_outgoing.send(pkl_package)
                        else:
                            logger.debug("[MAIN] Suppressing bad registration from manager:{}".format(
                                manager))

                else:
                    tasks_requested = int.from_bytes(message[1], "little")
                    logger.debug("[MAIN] Manager {} requested {} tasks".format(manager, tasks_requested))
                    self._ready_manager_queue[manager]['last'] = time.time()
                    if tasks_requested == HEARTBEAT_CODE:
                        logger.debug("[MAIN] Manager {} sends heartbeat".format(manager))
                        self.task_outgoing.send_multipart([manager, b'', PKL_HEARTBEAT_CODE])
                    else:
                        self._ready_manager_queue[manager]['free_capacity'] = tasks_requested
                        interesting_managers.add(manager)
                logger.debug("[MAIN] leaving task_outgoing section")

            # If we had received any requests, check if there are tasks that could be passed

            logger.debug("Managers count (total/interesting): {}/{}".format(len(self._ready_manager_queue),
                                                                            len(interesting_managers)))

            if interesting_managers and not self.pending_task_queue.empty():
                shuffled_managers = list(interesting_managers)
                random.shuffle(shuffled_managers)
                while shuffled_managers and not self.pending_task_queue.empty():  # cf. the if statement above...
                    manager = shuffled_managers.pop()
                    if (self._ready_manager_queue[manager]['free_capacity'] and
                        self._ready_manager_queue[manager]['active']):
                        tasks = self.get_tasks(self._ready_manager_queue[manager]['free_capacity'])
                        if tasks:
                            self.task_outgoing.send_multipart([manager, b'', pickle.dumps(tasks)])
                            task_count = len(tasks)
                            count += task_count
                            tids = [t['task_id'] for t in tasks]
                            logger.debug("[MAIN] Sent tasks: {} to {}".format(tids, manager))
                            self._ready_manager_queue[manager]['free_capacity'] -= task_count
                            self._ready_manager_queue[manager]['tasks'].extend(tids)
                            logger.info("[MAIN] Sent tasks: {} to manager {}".format(tids, manager))
                            if self._ready_manager_queue[manager]['free_capacity'] > 0:
                                logger.info("[MAIN] Manager {} still has free_capacity {}".format(manager, self._ready_manager_queue[manager]['free_capacity']))
                                # ... so keep it in the interesting_managers list
                            else:
                                logger.info("[MAIN] Manager {} is now saturated".format(manager))
                                interesting_managers.remove(manager)
                    else:
                        interesting_managers.remove(manager)
                        # logger.debug("Nothing to send to manager {}".format(manager))
                logger.debug("[MAIN] leaving _ready_manager_queue section, with {} managers still interesting".format(len(interesting_managers)))
            else:
                logger.debug("[MAIN] either no interesting managers or no tasks, so skipping manager pass")
            # Receive any results and forward to client
            if self.results_incoming in self.socks and self.socks[self.results_incoming] == zmq.POLLIN:
                logger.debug("[MAIN] entering results_incoming section")
                manager, *b_messages = self.results_incoming.recv_multipart()
                if manager not in self._ready_manager_queue:
                    logger.warning("[MAIN] Received a result from a un-registered manager: {}".format(manager))
                else:
                    logger.debug("[MAIN] Got {} result items in batch".format(len(b_messages)))
                    for b_message in b_messages:
                        r = pickle.loads(b_message)
                        # logger.debug("[MAIN] Received result for task {} from {}".format(r['task_id'], manager))
                        self._ready_manager_queue[manager]['tasks'].remove(r['task_id'])
                    self.results_outgoing.send_multipart(b_messages)
                    logger.debug("[MAIN] Current tasks: {}".format(self._ready_manager_queue[manager]['tasks']))
                logger.debug("[MAIN] leaving results_incoming section")

            # logger.debug("[MAIN] entering bad_managers section")
            bad_managers = [manager for manager in self._ready_manager_queue if
                            time.time() - self._ready_manager_queue[manager]['last'] > self.heartbeat_threshold]
            for manager in bad_managers:
                logger.debug("[MAIN] Last: {} Current: {}".format(self._ready_manager_queue[manager]['last'], time.time()))
                logger.warning("[MAIN] Too many heartbeats missed for manager {}".format(manager))
                e = ManagerLost(manager)
                for tid in self._ready_manager_queue[manager]['tasks']:
                    result_package = {'task_id': tid, 'exception': serialize_object(e)}
                    pkl_package = pickle.dumps(result_package)
                    self.results_outgoing.send(pkl_package)
                    logger.warning("[MAIN] Sent failure reports, unregistering manager")
                self._ready_manager_queue.pop(manager, 'None')
            logger.debug("[MAIN] ending one main loop iteration")

        delta = time.time() - start
        logger.info("Processed {} tasks in {} seconds".format(count, delta))
        logger.warning("Exiting")

    def compose_worker_launch_cmd(self):
        """ Compose the launch command
        """
        debug_opts = "--debug" if self.worker_debug else ""
        max_workers = "" if self.max_workers == float('inf') else "--max_workers={}".format(self.max_workers)

        l_cmd = self.launch_cmd.format(debug=debug_opts,
                                       task_url=self.worker_task_url,
                                       result_url=self.worker_result_url,
                                       cores_per_worker=self.cores_per_worker,
                                       max_workers=max_workers,
                                       nodes_per_block=self.provider.nodes_per_block,
                                       heartbeat_period=self.heartbeat_period,
                                       heartbeat_threshold=self.heartbeat_threshold,
                                       poll_period=self.poll_period,
                                       logdir="{}/{}".format(self.run_dir, self.label),
                                       worker_mode=self.worker_mode,
                                       container_image=self.container_image)
        self.launch_cmd = l_cmd
        logger.debug("Launch command: {}".format(self.launch_cmd))

    def scale_out(self, blocks=1):
        """Scales out the number of blocks by "blocks"

        Raises:
             NotImplementedError
        """
        r = []
        for i in range(blocks):
            if self.provider:
                block = self.provider.submit(self.launch_cmd, 1, 1)
                logger.debug("Launched block {}:{}".format(i, block))
                if not block:
                    raise(ScalingFailed(self.provider.label,
                                        "Attempts to provision nodes via provider has failed"))
                self.blocks.extend([block])
            else:
                logger.error("No execution provider available")
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


def start_file_logger(filename, name='interchange', level=logging.DEBUG, format_string=None):
    """Add a stream log handler.

    Parameters
    ---------

    filename: string
        Name of the file to write logs to. Required.
    name: string
        Logger name. Default="parsl.executors.interchange"
    level: logging.LEVEL
        Set the logging level. Default=logging.DEBUG
        - format_string (string): Set the format string
    format_string: string
        Format string to use.

    Returns
    -------
        None.
    """
    if format_string is None:
        format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d [%(levelname)s]  %(message)s"

    global logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def starter(comm_q, *args, **kwargs):
    """Start the interchange process

    The executor is expected to call this function. The args, kwargs match that of the Interchange.__init__
    """
    # logger = multiprocessing.get_logger()
    ic = Interchange(*args, **kwargs)
    comm_q.put((ic.worker_task_port,
                ic.worker_result_port))
    ic.start()


def cli_run():

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--client_address", required=True,
                        help="Client address")
    parser.add_argument("--client_ports", required=True,
                        help="client ports as a triple of outgoing,incoming,command")
    parser.add_argument("--worker_port_range",
                        help="Worker port range as a tuple")
    parser.add_argument("-l", "--logdir", default="./parsl_worker_logs",
                        help="Parsl worker log directory")
    parser.add_argument("-p", "--poll_period",
                        help="REQUIRED: poll period used for main thread")
    parser.add_argument("--worker_ports", default=None,
                        help="OPTIONAL, pair of workers ports to listen on, eg --worker_ports=50001,50005")
    parser.add_argument("--suppress_failure", action='store_true',
                        help="Enables suppression of failures")
    parser.add_argument("--endpoint_id", default=None,
                        help="Endpoint ID, used to identify the endpoint to the remote broker")
    parser.add_argument("--hb_threshold",
                        help="Heartbeat threshold in seconds")
    parser.add_argument("--config", default=None,
                        help="Configuration object that describes provisioning")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Enables debug logging")

    print("Starting HTEX Intechange")
    args = parser.parse_args()

    optionals = {}
    optionals['suppress_failure'] = args.suppress_failure
    optionals['logdir'] = os.path.abspath(args.logdir)
    optionals['client_address'] = args.client_address
    optionals['client_ports'] = [int(i) for i in args.client_ports.split(',')]
    optionals['endpoint_id'] = args.endpoint_id
    optionals['config'] = args.config

    if args.debug:
        optionals['logging_level'] = logging.DEBUG
    if args.worker_ports:
        optionals['worker_ports'] = [int(i) for i in args.worker_ports.split(',')]
    if args.worker_port_range:
        optionals['worker_port_range'] = [int(i) for i in args.worker_port_range.split(',')]

    with daemon.DaemonContext():
        ic = Interchange(**optionals)
        ic.start()
