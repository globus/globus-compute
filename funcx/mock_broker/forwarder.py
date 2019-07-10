import logging
import threading
from functools import partial
import uuid
import os
import queue

from multiprocessing import Process
from parsl.app.errors import RemoteExceptionWrapper
from funcx import set_file_logger

def double(x):
    return x*2

def failer(x):
    return x/0

class Forwarder(Process):
    """ Forwards tasks/results between the executor and the queues

        Tasks_Q  Results_Q
           |     ^
           |     |
           V     |
          Executors

    Todo : We need to clarify what constitutes a task that comes down
    the task pipe. Does it already have the code fragment? Or does that need to be sorted
    out from some DB ?
    """

    def __init__(self, task_q, result_q, executor, endpoint_id,
                 logdir="forwarder", logging_level=logging.INFO):
        """
        Params:
             task_q : A queue object
                Any queue object that has get primitives. This must be a thread-safe queue.
             result_q : A queue object
                Any queue object that has put primitives. This must be a thread-safe queue.
             executor: Executor object
                Executor to which tasks are to be forwarded
             endpoint_id: str
                Usually a uuid4 as string that identifies the executor
             logdir: str
                Path to logdir
             logging_level: int
        """
        super().__init__()
        self.logdir = logdir
        os.makedirs(self.logdir, exist_ok=True)

        global logger
        logger = set_file_logger(os.path.join(self.logdir, "forwarder.{}.log".format(endpoint_id)),
                        level=logging_level)

        logger.info("Initializing forwarder for endpoint:{}".format(endpoint_id))
        self.task_q = task_q
        self.result_q = result_q
        self.executor = executor
        self.endpoint_id = endpoint_id

    def handle_app_update(self, task_id, future):
        """ Triggered when the executor sees a task complete.

        This can be further optimized at the executor level, where we trigger this
        or a similar function when we see a results item inbound from the interchange.
        """
        logger.debug("[RESULTS] Updating result")
        try:
            res = future.result()
            self.result_q.put(res)
        except Exception as e:
            logger.debug("Task:{} failed".format(task_id))
            # Todo : Since we caught an exception, we should wrap it here, and send it
            # back onto the results queue.
        else:
            logger.debug("Task:{} succeeded".format(task_id))


    def run(self):
        """ Process entry point.
        """
        logger.info("[TASKS] Loop starting")
        logger.info("[TASKS] Executor: {}".format(self.executor))
        self.executor.start()
        while True:
            try:
                task = self.task_q.get(timeout=1)
            except queue.Empty:
                # This exception catching isn't very general,
                # Essentially any timeout exception should be caught and ignored
                pass
            else:
                # TODO: We are piping down a mock task. This needs to be fixed.
                task_id = str(uuid.uuid4())
                args = [5]
                kwargs = {}
                fu = self.executor.submit(double, *args, **kwargs)
                fu.add_done_callback(partial(self.handle_app_update, task_id))

        logger.info("[TASKS] Terminating self due to user requested kill")
        return


    def start_as_thread(self):
        logger.info("[Broker] Attempting start as threads")
        self._kill_event = threading.Event()
        self.threaded_self = threading.Thread(target=self.run,
                                              args=(self._kill_event,))
        self.threaded_self.start()

    def stop_thread(self):
        logger.info("[Broker] Setting kill event")
        self._kill_event.set()
        self.threaded_self.join()


def test_1():


    from parsl.executors import ThreadPoolExecutor
    import queue
    import time

    task_q = queue.Queue()
    result_q = queue.Queue()
    executor = ThreadPoolExecutor(max_threads=4)

    fw = Forwarder(task_q, result_q, executor, "Endpoint_01")
    fw.start()
    """
    kill_event = threading.Event()

    fw_thread = threading.Thread(target=fw.start_threaded,
                                 args=(kill_event,))
    fw_thread.start()
    """
    for i in range(100):
        print("Putting task onto task_q")
        task_q.put(b'fooooo')
        if not result_q.empty():
            result = result_q.get()
            print("Got this back from result_q: ", result)

    print(fw)
    print("Test done")


def test(ident=None):

    from multiprocessing import Queue
    from parsl.executors import ThreadPoolExecutor
    import time
    task_q = Queue()
    result_q = Queue()
    executor = ThreadPoolExecutor(max_threads=4)

    fw = Forwarder(task_q, result_q, executor, "Endpoint_{}".format(uuid.uuid4()))
    fw.start()

    """
    kill_event = threading.Event()

    fw_thread = threading.Thread(target=fw.start_threaded,
                                 args=(kill_event,))
    fw_thread.start()
    """
    for i in range(100):
        print("Putting task onto task_q")
        task_q.put(b'fooooo')
        if not result_q.empty():
            result = result_q.get()
            print("Got this back from result_q: ", result)

    print(fw)
    print(fw.terminate())
    print("Test done")



def spawn_forwarder():
    """ Spawns a forwarder and returns the forwarder process for tracking.

    Returns:
         A Forwarder object
    """
    from multiprocessing import Queue
    from parsl.executors import ThreadPoolExecutor

    import time
    task_q = Queue()
    result_q = Queue()
    executor = ThreadPoolExecutor(max_threads=4)

    fw = Forwarder(task_q, result_q, executor, "Endpoint_{}".format(uuid.uuid4()))
    fw.start()
    print("Test done")
    return fw


if __name__ == "__main__":

    test()


