import threading
import platform
import logging
import pickle
import parsl
import time
import json
import queue

from funcx.sdk.client import FuncXClient
from funcx.endpoint.utils.zmq_worker import ZMQWorker
from funcx.endpoint.config import (_get_parsl_config, _load_auth_client, _get_executor)
from funcx.sdk.config import lookup_option, write_option

from funcx.executor.high_throughput.executor import HighThroughputExecutor
from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.config import Config

from parsl.app.app import python_app

logging.basicConfig(filename='funcx_endpoint.log', level=logging.DEBUG)


def execute_function(code, entry_point, event=None):
    """Run the function. First it exec's the function code
    to load it into the current context and then eval's the function
    using the entry point

    Parameters
    ----------
    code : str
        The function code in a string format.
    entry_point : str
        The name of the function's entry point.
    event : dict
        The event context

    Returns
    -------
    json
        The result of running the function
    """
    try:
        exec(code)
        return eval(entry_point)(event)
    except Exception as e:
        return str(e)


class FuncXEndpoint:

    def __init__(self,
                 ip="funcx.org",
                 port=50001,
                 worker_threads=1,
                 container_type="Singularity",
                 endpoint_type="FuncX",
        ):
        """Initiate a funcX endpoint

        Parameters
        ----------
        ip : int
            IP address of the service to receive tasks
        port : int
            Port of the service to receive tasks
        worker_threads : int
            Number of concurrent workers to receive and process tasks
        container_type : str
            The virtualization type to use (Singularity, Shifter, Docker)
        """

        # Log in and start a client
        self.fx = FuncXClient()

        self.ip = ip
        self.port = port
        self.worker_threads = worker_threads
        self.container_type = container_type
        # Keep track of staged containers
        self.staged_containers = set()

        # Register this endpoint with funcX
        self.endpoint_uuid = lookup_option("endpoint_uuid")
        self.endpoint_uuid = self.fx.register_endpoint(platform.node(), self.endpoint_uuid)
        print(f"Endpoint UUID: {self.endpoint_uuid}")
        write_option("endpoint_uuid", self.endpoint_uuid)

        # Start parsl
        self.dfk = parsl.load(_get_parsl_config())

        # Start the endpoint
        self.endpoint_worker()

    def endpoint_worker(self):
        """The funcX endpoint worker. This initiates a funcX client and starts worker threads to:
        1. receive ZMQ messages (zmq_worker)
        2. perform function executions (execution_workers)
        3. return results (result_worker)

        We have two loops, one that persistently listens for tasks
        and the other that waits for task completion and send results

        Returns
        -------
        None
        """

        logging.info(f"Endpoint ID: {self.endpoint_uuid}")
        endpoint_worker = ZMQWorker("tcp://{}:{}".format(self.ip, self.port), self.endpoint_uuid)
        task_q = queue.Queue()
        result_q = queue.Queue()
        threads = []
        for i in range(1):
            thread = threading.Thread(target=self.execution_worker, args=(task_q, result_q,))
            thread.daemon = True
            threads.append(thread)
            thread.start()

        thread = threading.Thread(target=self.result_worker, args=(endpoint_worker, result_q,))
        thread.daemon = True
        threads.append(thread)
        thread.start()

        while True:
            (request, reply_to) = endpoint_worker.recv()
            task_q.put((request, reply_to))

    def _stage_container(self, container_uuid, container_type):
        """Stage the set of containers for local use.

        Parameters
        ----------
        endpoint_containers : dict
            A dictionary of containers to have locally for deployment

        Returns
        -------
        None
        """
        container = self.fx.get_container(container_uuid=container_uuid, container_type=container_type)
        if container['type'] == 'docker':      # docker container -- kubernetes will handle that
            container['local'] = None
        else:                                  # singularity or shifter
           pass
        self.staged_containers.add(container['container_uuid'])
        return container

    def execution_worker(self, task_q, result_q):
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
                container_uuid = to_do[-1]['container_uuid']
                if container_uuid:
                    if container_uuid not in self.staged_containers:
                        container = self._stage_container(container_uuid, self.container_type)
                        print("Staged container {}".format(container))
                        executor = _get_executor(container)
                        self.dfk.add_executors(executor)
                        print("added new executors")
                    app = python_app(execute_function, executors=[container_uuid]) 
                else:
                    app = python_app(execute_function, executors=['htex_local'])
                result = pickle.dumps(app(code, entry_point, event=event).result())
                print(pickle.loads(result))
                result_q.put(([result], reply_to))

    def result_worker(self, zmq_worker, result_q):
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

        while True:
            (result, reply_to) = result_q.get()
            zmq_worker.send(result, reply_to)


def start_endpoint():
    logging.debug("Starting endpoint")
    ep = FuncXEndpoint(ip='funcX.org', port=50001, container_type='docker')


if __name__ == "__main__":
    start_endpoint()
