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

import logging

parsl.load(_get_parsl_config())

dfk = parsl.dfk()
ex = dfk.executors['htex_local']

@python_app
def run_code(code, entry_point, event=None):
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


def send_request(serving_url, inputs):
    """Send an http POST request to the URL.

    Parameters
    ----------
    serving_url : string
        The url to send the request to
    inputs : dict
        A dictionary of input data to send as the request payload

    Returns
    -------
    dict
        The response of the request
    """
    headers = {"content-type": "application/json"}
    r = requests.post(serving_url, data=json.dumps(inputs), headers=headers)
    return json.loads(r.content)


def server(ip, port):
    """We have two loops, one that persistently listens for tasks
    and the other that waits for task completion and send results

    Parameters
    ----------
    ip : string
        The IP address of the service to receive tasks
    port : int
        The port to connect to the service

    Returns
    -------
    None
    """

    # Log into funcX via globus
    fx = FuncXClient()

    # Register this endpoint with funcX
    uuid = fx.register_endpoint(platform.node())
    # print(uuid)

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
    """Worker thread to send results back to broker

    Parameters
    ----------
    endpoint_worker : Thread
        The worker thread
    result_q : list
        The queue to add results to.

    Returns
    -------
    None
    """

    counter = 0
    while True:
        ta = time.time()
        (result, reply_to)= result_q.get()
        #print(result, reply_to)
        endpoint_worker.send(result, reply_to)
        tb = time.time()
        counter += 1


def parsl_worker(task_q, result_q):
    """

    Parameters
    ----------
    task_q
    result_q

    Returns
    -------

    """

    exec_times = []
    endpoint_times = []
    while True:
         
        if task_q:
            #t0 = time.time()
            request, reply_to = task_q.get()
            t0 = time.time()
            to_do = pickle.loads(request[0])
            code, entry_point, event = to_do[-1]['function'], to_do[-1]['entry_point'], to_do[-1]['event']
            # print(code, entry_point, event)
            ###################
            ## TIMER THIS FOR CODE EXECUTION TIME ##
            #t0 = time.time()
            result = pickle.dumps(run_code(code, entry_point, event=event).result())
            #t1 = time.time()

            # exec_times.append(t1-t0)

            #if len(exec_times) > 5:
            #    print("Exec number: {}".format(len(exec_times)))
            #    print("Execution time: {}".format(t1-t0))
            #    print("Mean/STDEV: {}/{}".format(statistics.mean(exec_times), statistics.stdev(exec_times)))

            ####################

            # print("result is {}".format(result))
            result_q.put(([result], reply_to))
            t1 = time.time()

            endpoint_times.append(t1-t0)

            if len(endpoint_times) > 19:
                print("Mean/STDEV: {}/{}".format(statistics.mean(endpoint_times), statistics.stdev(endpoint_times)))
            

def worker(ip, port, identity):
    """
    Worker threads to process requests

    :return: None
    """

    # TODO: Make the zmq_client kinda match the server -> threads -> clients.
    serv = ZmqClient(ip, port, identity)
    count = 0

    endpoint_times = []
    while True:
        t0 = time.time()
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
        t1 = time.time()
        endpoint_times.append(t1-t0)

        if count > 20:
            with open('/home/ubuntu/endpoint_time.csv', 'w') as f:
                f.write("Mean/STDEV: {}/{}".format(statistics.mean(endpoint_times), statistics.stdev(endpoint_times)))
                exit()


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

    # print("Runner: Executing Command: " + str(cmd))

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
        print('that was the future, this is the result function')
        x = fut_result.result()
        print(type(x))

        return json.dumps({"status": "SUCCESS", "result": x})
    return json.dumps({"status": "PROCESSING"})


def main():
    server('funcX.org', 50001)


if __name__ == "__main__":
    main()

