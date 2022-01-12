#!/usr/bin/env python3

import argparse
import logging
import os
import pickle
import signal
import sys

import zmq
from parsl.app.errors import RemoteExceptionWrapper

from funcx.serialize import FuncXSerializer
from funcx_endpoint.executors.high_throughput.messages import Message
from funcx_endpoint.logging_config import setup_logging

try:
    from funcx.utils.errors import MaxResultSizeExceeded
except ImportError:
    # to-do: Remove this after funcx,funcx-endpoint==0.3.5 is released
    class MaxResultSizeExceeded(Exception):
        """Result produced by the function exceeds the maximum supported result size
        threshold"""

        def __init__(self, result_size: int, result_size_limit: int):
            self.result_size = result_size
            self.result_size_limit = result_size_limit

        def __str__(self) -> str:
            return (
                f"Task result of {self.result_size}B exceeded current "
                f"limit of {self.result_size_limit}B"
            )


log = logging.getLogger(__name__)

DEFAULT_RESULT_SIZE_LIMIT_MB = 10
DEFAULT_RESULT_SIZE_LIMIT_B = DEFAULT_RESULT_SIZE_LIMIT_MB * 1024 * 1024


class FuncXWorker:
    """The FuncX worker
    Parameters
    ----------

    worker_id : str
     Worker id string

    address : str
     Address at which the manager might be reached. This is usually 127.0.0.1

    port : int
     Port at which the manager can be reached

    result_size_limit : int
     Maximum result size allowed in Bytes
     Default = 10 MB

    Funcx worker will use the REP sockets to:
         task = recv ()
         result = execute(task)
         send(result)
    """

    def __init__(
        self,
        worker_id,
        address,
        port,
        worker_type="RAW",
        result_size_limit=DEFAULT_RESULT_SIZE_LIMIT_B,
    ):

        self.worker_id = worker_id
        self.address = address
        self.port = port
        self.worker_type = worker_type
        self.serializer = FuncXSerializer()
        self.serialize = self.serializer.serialize
        self.deserialize = self.serializer.deserialize
        self.result_size_limit = result_size_limit

        log.info(f"Initializing worker {worker_id}")
        log.info(f"Worker is of type: {worker_type}")

        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.identity = worker_id.encode()

        self.task_socket = self.context.socket(zmq.DEALER)
        self.task_socket.setsockopt(zmq.IDENTITY, self.identity)

        log.info(f"Trying to connect to : tcp://{self.address}:{self.port}")
        self.task_socket.connect(f"tcp://{self.address}:{self.port}")
        self.poller.register(self.task_socket, zmq.POLLIN)
        signal.signal(signal.SIGTERM, self.handler)

    def handler(self, signum, frame):
        log.error("Signal handler called with signal", signum)
        sys.exit(1)

    def registration_message(self):
        return {"worker_id": self.worker_id, "worker_type": self.worker_type}

    def start(self):

        log.info("Starting worker")

        result = self.registration_message()
        task_type = b"REGISTER"
        log.debug("Sending registration")
        self.task_socket.send_multipart(
            [task_type, pickle.dumps(result)]  # Byte encoded
        )

        while True:

            log.debug("Waiting for task")
            p_task_id, p_container_id, msg = self.task_socket.recv_multipart()
            task_id = pickle.loads(p_task_id)
            container_id = pickle.loads(p_container_id)
            log.debug(f"Received task_id:{task_id} with task:{msg}")

            result = None
            task_type = None
            if task_id == "KILL":
                task = Message.unpack(msg)
                if task.task_buffer.decode("utf-8") == "KILL":
                    log.info("[KILL] -- Worker KILL message received! ")
                    task_type = b"WRKR_DIE"
                else:
                    log.exception(
                        "Caught an exception of non-KILL message for KILL task"
                    )
                    continue
            else:
                log.debug("Executing task...")

                try:
                    result = self.execute_task(msg)
                    serialized_result = self.serialize(result)

                    if len(serialized_result) > self.result_size_limit:
                        raise MaxResultSizeExceeded(
                            len(serialized_result), self.result_size_limit
                        )
                except Exception as e:
                    log.exception(f"Caught an exception {e}")
                    result_package = {
                        "task_id": task_id,
                        "container_id": container_id,
                        "exception": self.serialize(
                            RemoteExceptionWrapper(*sys.exc_info())
                        ),
                    }
                else:
                    log.debug("Execution completed without exception")
                    result_package = {
                        "task_id": task_id,
                        "container_id": container_id,
                        "result": serialized_result,
                    }
                result = result_package
                task_type = b"TASK_RET"

            log.debug("Sending result")

            self.task_socket.send_multipart(
                [task_type, pickle.dumps(result)]  # Byte encoded
            )

            if task_type == b"WRKR_DIE":
                log.info(f"*** WORKER {self.worker_id} ABOUT TO DIE ***")
                # Kill the worker after accepting death in message to manager.
                sys.exit()
                # We need to return here to allow for sys.exit mocking in tests
                return

        log.warning("Broke out of the loop... dying")

    def execute_task(self, message):
        """Deserialize the buffer and execute the task.

        Returns the result or throws exception.
        """
        task = Message.unpack(message)
        f, args, kwargs = self.serializer.unpack_and_deserialize(
            task.task_buffer.decode("utf-8")
        )
        return f(*args, **kwargs)


def cli_run():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-w", "--worker_id", required=True, help="ID of worker from process_worker_pool"
    )
    parser.add_argument(
        "-t", "--type", required=False, help="Container type of worker", default="RAW"
    )
    parser.add_argument(
        "-a", "--address", required=True, help="Address for the manager, eg X,Y,"
    )
    parser.add_argument(
        "-p",
        "--port",
        required=True,
        help="Internal port at which the worker connects to the manager",
    )
    parser.add_argument(
        "--logdir", required=True, help="Directory path where worker log files written"
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="Directory path where worker log files written",
    )
    args = parser.parse_args()

    os.makedirs(args.logdir, exist_ok=True)
    setup_logging(
        logfile=os.path.join(args.logdir, f"funcx_worker_{args.worker_id}.log"),
        debug=args.debug,
    )

    worker = FuncXWorker(
        args.worker_id,
        args.address,
        int(args.port),
        worker_type=args.type,
    )
    worker.start()


if __name__ == "__main__":
    cli_run()
