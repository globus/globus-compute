
from queue import Queue
import logging
import random
import subprocess

logger = logging.getLogger(__name__)


class WorkerMap(object):
    """ WorkerMap keeps track of workers
    """

    def __init__(self, worker_count):
        self.worker_count = worker_count
        self.total_worker_type_counts = {'slots': self.worker_count, 'RAW': 0}
        self.ready_worker_type_counts = {}
        self.worker_queues = {}
        self.worker_types = {}
        self.worker_counter = 0  # used to create worker_ids

        # Only spin up containers if active_workers + pending_workers < max_workers.
        self.active_workers = 0
        self.pending_workers = 0

        # Need to keep track of workers that are ABOUT to die
        self.to_die_count = {'RAW': 0}

    def register_worker(self, worker_id, worker_type):
        """ Add a new worker
        """
        logger.debug("In register worker worker_id: {} type:{}".format(worker_id, worker_type))
        self.worker_types[worker_id] = worker_type

        if worker_type not in self.worker_queues:
            self.worker_queues[worker_type] = Queue()

        self.total_worker_type_counts[worker_type] = self.total_worker_type_counts.get(worker_type, 0) + 1
        self.ready_worker_type_counts[worker_type] = self.ready_worker_type_counts.get(worker_type, 0) + 1
        self.total_worker_type_counts['slots'] -= 1
        self.worker_queues[worker_type].put(worker_id)

        if worker_type not in self.to_die_count:
            self.to_die_count[worker_type] = 0

    def remove_worker(self, worker_id):
        """ Remove the worker from the WorkerMap

            Should already be KILLed by this point.
        """

        worker_type = self.worker_types[worker_id]

        self.active_workers -= 1
        self.total_worker_type_counts[worker_type] -= 1
        self.to_die_count[worker_type] -= 1
        self.total_worker_type_counts['slots'] += 1

    def spin_up_workers(self, next_worker_q, address=None, debug=None, uid=None, logdir=None, worker_port=None):
        """ Helper function to call 'remove' for appropriate workers in 'new_worker_map'.

        Parameters
        ----------
        new_worker_q : queue.Queue()
           Queue of worker types to be spun up next.
        address : str
            Address at which to connect to the workers.
        debug : bool
            Whether debug logging is activated.
        uid : str
            Worker ID to be assigned to worker.
        logdir: str
            Directory in which to write logs
        worker_port: int
            Port at which to connect to the workers.

        Returns
        ---------
        Total number of spun-up workers.
        """
        spin_ups = 0

        logger.info("[SPIN UP] Next Worker Qsize: {}".format(len(next_worker_q)))
        logger.info("[SPIN UP] Active Workers: {}".format(self.active_workers))
        logger.info("[SPIN UP] Pending Workers: {}".format(self.pending_workers))
        logger.info("[SPIN UP] Max Worker Count: {}".format(self.worker_count))

        if len(next_worker_q) > 0 and self.active_workers + self.pending_workers < self.worker_count:
            logger.debug("[SPIN UP] Spinning up new workers!")
            num_slots = min(self.worker_count - self.active_workers - self.pending_workers, len(next_worker_q))
            for _ in range(num_slots):

                try:
                    self.add_worker(worker_id=str(self.worker_counter),
                                    worker_type=next_worker_q.pop(0),
                                    address=address, debug=debug,
                                    uid=uid,
                                    logdir=logdir,
                                    worker_port=worker_port)
                except Exception:
                    logger.exception("Error spinning up worker! Skipping...")
                    continue
                else:
                    spin_ups += 1
                    self.pending_workers += 1
                self.worker_counter += 1
        return spin_ups

    def spin_down_workers(self, new_worker_map):
        """ Helper function to call 'remove' for appropriate workers in 'new_worker_map'.

        Parameters
        ----------
        new_worker_map : dict
           {worker_type: total_number_of_containers,...}.

        Returns
        ---------
        List of removed worker types.
        """

        spin_downs = []
        for worker_type in new_worker_map:
            if new_worker_map[worker_type] < self.total_worker_type_counts[worker_type]:
                num_remove = self.total_worker_type_counts[worker_type] - new_worker_map[worker_type]

                logger.info("[WORKER_REMOVE] Removing {} workers of type {}".format(num_remove, worker_type))
                for i in range(num_remove):
                    spin_downs.append(worker_type)
        return spin_downs

    def add_worker(self, worker_id=str(random.random()),
                   mode='no_container',
                   worker_type='RAW',
                   container_uri=None,
                   walltime=1,
                   address=None,
                   debug=None,
                   worker_port=None,
                   logdir=None,
                   uid=None):
        """ Launch the appropriate worker

        Parameters
        ----------
        worker_id : str
           Worker identifier string
        mode : str
           Valid options are no_container, singularity
        walltime : int
           Walltime in seconds before we check status

        """

        debug = ' --debug' if debug else ''

        worker_id = ' --worker_id {}'.format(worker_id)

        cmd = (f'funcx-worker {debug}{worker_id} '
               f'-a {address} '
               f'-p {worker_port} '
               f'-t {worker_type} '
               f'--logdir={logdir}/{uid} ')

        logger.info("Command string :\n {}".format(cmd))

        if mode == 'no_container':
            modded_cmd = cmd
        elif mode == 'singularity':
            modded_cmd = f'singularity run --writable {container_uri} {cmd}'
        else:
            raise NameError("Invalid container launch mode.")

        try:
            proc = subprocess.Popen(modded_cmd.split(),
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    shell=False)

        except Exception:
            logger.exception("Got an error in worker launch")
            raise

        return proc

    def get_next_worker_q(self, new_worker_map):
        """ Helper function to generate a queue of next workers to spin up .
            From a mapping generated by the scheduler

        Parameters
        ----------
        new_worker_map : dict
           {worker_type: total_number_of_containers,...}

        Returns
        ---------
        Queue containing the next workers the system should spin-up.
        """

        # next_worker_q = []
        new_worker_list = []
        for worker_type in new_worker_map:
            # If we don't already have this type of worker in our worker_map...
            if worker_type not in self.total_worker_type_counts:
                self.total_worker_type_counts[worker_type] = 0
            if new_worker_map[worker_type] > self.total_worker_type_counts[worker_type]:

                for i in range(new_worker_map[worker_type] - self.total_worker_type_counts[worker_type]):
                    # Add worker
                    new_worker_list.append(worker_type)

        # Randomly assign order of newly needed containers... add to spin-up queue.
        if len(new_worker_list) > 0:
            random.shuffle(new_worker_list)

        return new_worker_list

    def put_worker(self, worker):
        """ Adds worker to the list of waiting workers
        """
        worker_type = self.worker_types[worker]

        if worker_type not in self.worker_queues:
            self.worker_queues[worker_type] = Queue()

        self.ready_worker_type_counts[worker_type] += 1
        self.worker_queues[worker_type].put(worker)

    def get_worker(self, worker_type):
        """ Get a task and reduce the # of worker for that type by 1.
        Raises queue.Empty if empty
        """
        worker = self.worker_queues[worker_type].get_nowait()
        self.ready_worker_type_counts[worker_type] -= 1
        return worker

    def get_worker_counts(self):
        """ Returns just the dict of worker_type and counts
        """
        return self.total_worker_type_counts

    def ready_worker_count(self):
        return sum(self.total_worker_type_counts.values())
