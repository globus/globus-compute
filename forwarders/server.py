from taskqueue import TaskQueue
import redis
import time
import zmq
from typing import Dict, Tuple

def test_1():
    """ Allows any client to connect, and sends them a hello message
    """
    task_q = TaskQueue('127.0.0.1', mode='server')

    while True:
        client, *message = task_q.get()
        print(f"Got {message} from {client}")
        print(f"Sending reply")
        task_q.put(client, b'hello')


def test_2():
    r = redis.Redis(host='localhost', port=6379, db=0)
    task_q = TaskQueue('127.0.0.1', mode='server')

    pubsub = r.pubsub()
    pubsub.subscribe('tasks_ep1')
    pubsub.subscribe('tasks_ep2')

    while True:
        package = pubsub.get_message(ignore_subscribe_messages=True, timeout=2)
        if package:
            print("Got package for : ", package['channel'], package)
            dest = package['channel']
            task_q.put(dest, package['data'])
        else:
            print("sleeping...")

def generate_keys(key_dir: str) -> Dict[str, Tuple[str, str]]:
    """Generate all public/private keys needed for this demo.
    Parameters
    ----------
    key_dir
        Directory to write keys to.
    """
    s_pub, s_sec = zmq.auth.create_certificates(key_dir, "server")
    c_pub, c_sec = zmq.auth.create_certificates(key_dir, "client")
    return {"server": (s_pub, s_sec), "client": (c_pub, c_sec)}


if __name__ == '__main__':

    generate_keys('/tmp')
    try:
        # test_1()
        test_2()
    except KeyboardInterrupt:
        print("Exiting from Keyboard interrupt")

