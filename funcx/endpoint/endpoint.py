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
from funcx.endpoint.config import (_get_parsl_config, _load_auth_client)

from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.app.app import python_app

parsl.load(_get_parsl_config())

dfk = parsl.dfk()
ex = dfk.executors['htex_local']

@python_app
def run_code(code, entry_point, event=None):
    exec(code)
    return eval(entry_point)(event)


def send_request(serving_url, inputs):
    headers = {"content-type": "application/json"}
    r = requests.post(serving_url, data=json.dumps(inputs), headers=headers)
    return json.loads(r.content)


def server(ip, port):
    """
    We have two loops, one that persistently listens for tasks
    and the other that waits for task completion and send results
    """

    # Log into funcX via globus
    fx = FuncXClient()

    # Register this endpoint with funcX
    uuid = fx.register_endpoint(platform.node())

    endpoint_worker = ZMQWorker("tcp://{}:{}".format(ip, port), uuid)
    reply = None
    task_q = queue.Queue()
    result_q = queue.Queue()
    threads = []
    for i in range(1):
        thread = threading.Thread(target=parsl_worker, args=(task_q, result_q,))
        thread.daemon = True
        threads.append(thread)
        thread.start()

    thread = threading.Thread(target=result_worker, args=(endpoint_worker, result_q, ))
    thread.daemon = True
    threads.append(thread)
    thread.start()

    while True:
        (request, reply_to) = endpoint_worker.recv()
        task_q.put((request, reply_to))


def result_worker(endpoint_worker, result_q):
    """Worker thread to send results back to broker"""
    counter = 0
    while True:
        (result, reply_to)= result_q.get()
        endpoint_worker.send(result, reply_to)

def parsl_worker(task_q, result_q):
    exec_times = []
    endpoint_times = []
    while True:
        if task_q:
            request, reply_to = task_q.get()
            t0 = time.time()
            to_do = pickle.loads(request[0])
            code, entry_point, event = to_do[-1]['function'], to_do[-1]['entry_point'], to_do[-1]['event']
            result = pickle.dumps(run_code(code, entry_point, event=event).result())
            result_q.put(([result], reply_to))


def main():
    server('funcX.org', 50001)

if __name__ == "__main__":
    main()

