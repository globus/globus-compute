import threading
import platform
import requests
import logging
import pickle
import parsl
import time
import json

from funcx_sdk.client import funcXClient
from utils.zmq_client import ZmqClient
from config import (_get_parsl_config, _load_auth_client)

from parsl.executors import HighThroughputExecutor
from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.config import Config


parsl.load(_get_parsl_config())

dfk = parsl.dfk()
ex = dfk.executors['htex_local']


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
    fx = funcXClient()

    # Register this endpoint with funcX
    fx.register_endpoint(platform.node())

    threads = []
    for i in range(10):
        thread = threading.Thread(target=worker, args=(ip, port,))
        thread.daemon = True
        threads.append(thread)
        thread.start()

    import time
    while True:
        time.sleep(1)


def worker(ip, port):
    """
    Worker threads to process requests

    :return: None
    """

    # TODO: Make the zmq_client kinda match the server -> threads -> clients.
    serv = ZmqClient(ip, port)
    count = 0

    while True:
        msg = serv.recv()
        count += 1
        (msg_type, site_id, task_inputs) = pickle.loads(msg)

        try:
            print(msg_type)
            print(site_id)
            print(task_inputs)
            invocation_start = time.time()
            # Invoke the servable
            fut = None
            reply = None

            process_time = 0.0
            cmd = task_inputs['command']

            fut = yadu_executor(cmd, site_id)

            reply = fut['result']

            invocation_end = time.time()
            # Append timing information
            invocation_time = (invocation_end - invocation_start) * 1000
            if invocation_time < process_time:
                process_time = 0.0
            response = {"process_time": process_time, "invocation_time": invocation_time, "response": reply}
            print(reply)
            serv.send(pickle.dumps(response))
        except Exception as e:
            serv.send(pickle.dumps({"Error": "Failed to invoke function."}))
            print(e)
        print("Checking %s > 20" % count)
        if count > 5:
            print("Flushing dfk tasks... ")
            parsl.dfk().tasks = {}
            count = 0


class NullHandler(logging.Handler):
    """Setup default logging to /dev/null since this is library."""

    def emit(self, record):
        pass


def run_command(command):

    import subprocess
    for x in range(0, 1):
        try:
            print('running cmd: {}'.format(command))
            process = subprocess.Popen(command.split(' '), stdout=subprocess.PIPE)
            out, err = process.communicate()
            ret_val = out.decode('utf-8').strip()
            if ret_val:
                return ret_val
        except Exception as e:
            print("Unexpected Exception: {}".format(str(e)))
            pass
    return 'done'


def yadu_executor(cmd, task_uuid):

    print("Runner: Executing Command: " + str(cmd))

    # TODO: Take actual arg here.
    is_async = False

    fut_result = ex.submit(run_command, cmd)
    print(fut_result)
    if not is_async:
        print("Waiting for task")
        while not fut_result.done():
            time.sleep(2)

        print('task done! updating things.')
        # _update_task(task_uuid, "SUCCEEDED")
        print(fut_result)
        print('that was the fut_result, this is the result function')
        print(fut_result.result())
        return json.dumps({"status": "SUCCESS", "result": fut_result.result()})
    return json.dumps({"status": "PROCESSING"})


if __name__ == "__main__":
    server('funcX.org', 50001)
