
from queue import Queue
import logging

logger = logging.getLogger(__name__)


class WorkerMap(object):
    """ WorkerMap keeps track of workers
    """
    def __init__(self, worker_count):
        self.worker_count = worker_count
        self.worker_counts = {'slots': self.worker_count, 'RAW': 0}
        self.ready_worker_counts = {}
        self.worker_queues = {}
        self.worker_types = {}

    def register_worker(self, worker_id, worker_type):
        """ Add a new worker
        """
        logger.debug("In register worker worker_id: {} type:{}".format(worker_id, worker_type))
        self.worker_types[worker_id] = worker_type

        if worker_type not in self.worker_queues:
            self.worker_queues[worker_type] = Queue()

        self.worker_counts[worker_type] = self.worker_counts.get(worker_type, 0) + 1
        self.ready_worker_counts[worker_type] = self.ready_worker_counts.get(worker_type, 0) + 1
        self.worker_counts['slots'] -= 1
        self.worker_queues[worker_type].put(worker_id)

    def remove_worker(self, worker_id):
        """ Remove the worker from the WorkerMap

            Should already be KILLed by this point. 
        """

        worker_type = self.worker_types[worker_id]

        self.worker_counts[worker_type] -= 1
        self.worker_counts['slots'] += 1

    def put_worker(self, worker):
        """ Adds worker to the list of waiting workers
        """
        worker_type = self.worker_types[worker]

        if worker_type not in self.worker_queues:
            self.worker_queues[worker_type] = Queue()

        self.ready_worker_counts[worker_type] += 1
        self.worker_queues[worker_type].put(worker)

    def get_worker(self, worker_type):
        """ Get a task and reduce the # of worker for that type by 1.
        Raises queue.Empty if empty
        """
        try:
            worker = self.worker_queues[worker_type].get_nowait()
        except Exception as e:
            raise
        else:
            # pass 
            self.ready_worker_counts[worker_type] -= 1

        return worker

    def get_worker_counts(self):
        """ Returns just the dict of worker_type and counts
        """
        return self.worker_counts

    def ready_worker_count(self):
        return sum(self.worker_counts.values())
