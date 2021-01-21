import argparse
import uuid
import zmq
from taskqueue import TaskQueue


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-e", "--endpoint_name", required=True,
                        help="Endpoint name")
    args = parser.parse_args()

    task_q = TaskQueue('127.0.0.1', identity=args.endpoint_name, mode='client')
    print("Waiting for message")
    print(task_q.register_client(b'hello from client'))
    while True:
        try:
            print(task_q.get(block=False, timeout=2000))
        except zmq.Again:
            print("No message")

