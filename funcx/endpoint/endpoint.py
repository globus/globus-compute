import statistics
import threading
import platform
import requests
import logging
import pickle
import parsl
import time
import json
import queue

from funcx.sdk.client import FuncXClient
from funcx.endpoint.utils.zmq_worker import ZMQWorker
from funcx.endpoint.config import (_get_parsl_config)

from parsl.app.app import python_app


parsl.load(_get_parsl_config())

logging.basicConfig(filename='funcx_endpoint.log', level=logging.DEBUG)

@python_app
def execute_function(code, entry_point, event=None):
    """Run the function. First it exec's the function code
    to load it into the current context and then eval's the function
    using the entry point

    Parameters
    ----------
    code : string
        The function code in a string format.
    entry_point : string
        The name of the function's entry point.
    event : dict
        The event context

    Returns
    -------
    json
        The result of running the function
    """
    exec(code)
    return eval(entry_point)(event)


def funcx_endpoint(ip="funcx.org", port=50001, worker_threads=1):
    """The funcX endpoint. This initiates a funcX client and starts worker threads to:
    1. receive ZMQ messages (zmq_worker)
    2. perform function executions (execution_workers)
    3. return results (result_worker)

    We have two loops, one that persistently listens for tasks
    and the other that waits for task completion and send results

    Parameters
    ----------
    ip : string
        The IP address of the service to receive tasks
    port : int
        The port to connect to the service
    worker_threads : int
        The number of worker threads to start.

    Returns
    -------
    None
    """

    # Log into funcX via globus
    fx = FuncXClient()

    # Register this endpoint with funcX
    endpoint_uuid = fx.register_endpoint(platform.node())
    logging.info(f"Endpoint ID: {endpoint_uuid}")

    zmq_worker = ZMQWorker("tcp://{}:{}".format(ip, port), endpoint_uuid)
    task_q = queue.Queue()
    result_q = queue.Queue()
    threads = []
    for i in range(worker_threads):
        thread = threading.Thread(target=execution_worker, args=(task_q, result_q,))
        thread.daemon = True
        threads.append(thread)
        thread.start()

    thread = threading.Thread(target=result_worker, args=(zmq_worker, result_q, ))
    thread.daemon = True
    threads.append(thread)
    thread.start()

    while True:
        (request, reply_to) = zmq_worker.recv()
        task_q.put((request, reply_to))


def execution_worker(task_q, result_q):
    """A worker thread to process tasks and place results on the
    result queue.

    Parameters
    ----------
    task_q : queue.Queue
        A queue of tasks to process.
    result_q : queue.Queue
        A queue to put return queues.

    Returns
    -------
    None
    """

    while True:
        if task_q:
            request, reply_to = task_q.get()

            to_do = pickle.loads(request[0])
            code, entry_point, event = to_do[-1]['function'], to_do[-1]['entry_point'], to_do[-1]['event']

            result = pickle.dumps(execute_function(code, entry_point, event=event).result())

            result_q.put(([result], reply_to))


def result_worker(zmq_worker, result_q):
    """Worker thread to send results back to funcX service via the broker.

    Parameters
    ----------
    zmq_worker : Thread
        The worker thread
    result_q : queue.Queue
        The queue to add results to.

    Returns
    -------
    None
    """

    counter = 0
    while True:
        (result, reply_to) = result_q.get()
        zmq_worker.send(result, reply_to)
        counter += 1


def main():
    funcx_endpoint('funcX.org', 50001)


if __name__ == "__main__":
    main()

