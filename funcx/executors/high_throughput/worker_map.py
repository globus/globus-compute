import funcx
from queue import Queue
import logging

logger = logging.getLogger(__name__)


class WorkerMap(object):
    """ WorkerMap keeps track of workers
    """
    def __init__(self, worker_count):
        logger.info("Start")
        self.worker_count = worker_count
        self.worker_counts = {'slots' : self.worker_count}
        self.worker_queues = {}
        self.worker_types = {}

    def register_worker(self, worker_id, worker_type):
        """ Add a new worker
        """
        logger.debug("In register worker worker_id: {} type:{}".format(worker_id, worker_type))
        self.worker_types[worker_id] = worker_type

        if worker_id in self.worker_counts:
            raise Exception("Worker already exists")

        if worker_type not in self.worker_counts:
            self.worker_counts[worker_type] = 0

        if worker_type not in self.worker_queues:
            self.worker_queues[worker_type] = Queue()

        self.worker_counts[worker_type] = self.worker_counts.get(worker_type, 0) + 1
        self.worker_counts['slots'] -= 1
        self.worker_queues[worker_type].put(worker_id)

    def put_worker(self, worker):
        """ Adds worker to the list of waiting workers
        """
        worker_type = self.worker_types[worker]

        if worker_type not in self.worker_queues:
            self.worker_queues[worker_type] = Queue()

        self.worker_counts[worker_type] += 1
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
            self.worker_counts[worker_type] -= 1

        return worker

    def get_worker_counts(self):
        """ Returns just the dict of worker_type and counts
        """
        return self.worker_counts

    def ready_worker_count(self):
        return sum(self.worker_counts.values())


if __name__ == "__main__":

    from funcx import set_stream_logger
    logger = set_stream_logger(level=logging.DEBUG)

    worker_map = WorkerMap(8)

    worker_map.register_worker(1, 'DEFAULT')
    worker_map.register_worker(2, 'CONT_1')

    worker_map.put_worker(1)
    worker_map.put_worker(2)

    print("Counts : ", worker_map.get_worker_counts())

    x = worker_map.get_worker('DEFAULT')
    print("Took worker of type {} : {}".format('DEFAULT', x))

    print("Counts : ", worker_map.get_worker_counts())

    print("Here")
