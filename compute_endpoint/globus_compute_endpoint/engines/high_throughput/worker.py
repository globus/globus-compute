from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import typing as t

import dill
import zmq
from globus_compute_common import messagepack
from globus_compute_common.messagepack.message_types import Result as OutgoingResult
from globus_compute_common.messagepack.message_types import (
    ResultErrorDetails as OutgoingResultErrorDetails,
)
from globus_compute_endpoint.engines.high_throughput.messages import Message
from globus_compute_endpoint.exception_handling import (
    get_error_string,
    get_result_error_details,
)
from globus_compute_endpoint.logging_config import setup_logging
from globus_compute_sdk.sdk.utils import get_env_details
from globus_compute_sdk.serialize import ComputeSerializer

log = logging.getLogger(__name__)

DEFAULT_RESULT_SIZE_LIMIT_MB = 10
DEFAULT_RESULT_SIZE_LIMIT_B = DEFAULT_RESULT_SIZE_LIMIT_MB * 1024 * 1024


class Worker:
    """The Globus Compute worker
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

    Globus Compute worker will use the REP sockets to:
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
        self.serializer = ComputeSerializer()
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
        log.error(f"Signal handler called with signal {signum}")
        sys.exit(1)

    def _send_registration_message(self):
        log.debug("Sending registration")
        payload = {"worker_id": self.worker_id, "worker_type": self.worker_type}
        self.task_socket.send_multipart([b"REGISTER", dill.dumps(payload)])

    def start(self):
        log.info("Starting worker")
        self._send_registration_message()

        while True:
            log.debug("Waiting for task")
            p_task_id, p_container_id, msg = self.task_socket.recv_multipart()
            task_id: str = dill.loads(p_task_id)
            container_id: str = dill.loads(p_container_id)
            log.debug(f"Received task with task_id='{task_id}' and msg='{msg}'")

            if task_id == "KILL":
                log.info("[KILL] -- Worker KILL message received! ")
                # send a "worker die" message back to the manager
                self.task_socket.send_multipart([b"WRKR_DIE", b""])
                log.info(f"*** WORKER {self.worker_id} ABOUT TO DIE ***")
                # Kill the worker after accepting death in message to manager.
                sys.exit()
            else:
                result = self._worker_execute_task(task_id, msg)
                result["container_id"] = container_id
                log.debug("Sending result")
                # send bytes over the socket back to the manager
                self.task_socket.send_multipart([b"TASK_RET", dill.dumps(result)])

        log.warning("Broke out of the loop... dying")

    def compose_exception_message(self, task_id: str) -> bytes:
        code, user_message = get_result_error_details()
        outgoing_result = OutgoingResult(
            task_id=task_id,
            data=get_error_string(),
            error_details=OutgoingResultErrorDetails(
                code=code,
                user_message=user_message,
            ),
            details=get_env_details(),
        )
        return messagepack.pack(outgoing_result)

    def _worker_execute_task(
        self, task_id: str, msg: bytes
    ) -> dict[str, t.Union[str, bytes]]:
        result_message: dict[str, t.Union[str, bytes]] = {"task_id": task_id}
        try:
            # Unwrap HTEX's Task packing
            task_message = Message.unpack(msg)
            serialized_fn_package = task_message.task_buffer.decode()

            # Deserialize HTEX Engines' wrapping of
            # execute_task, messagepack_payload)
            function, args, kwargs = self.deserialize(serialized_fn_package)

            # Execute
            serialized_result: bytes = function(*args, **kwargs)
            result_message["data"] = serialized_result

        except Exception:
            log.exception("Failed to execute task")
            serialized_error = self.compose_exception_message(task_id)
            result_message["data"] = serialized_error

        return result_message


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

    setup_logging(
        logfile=os.path.join(args.logdir, f"funcx_worker_{args.worker_id}.log"),
        debug=args.debug,
    )

    # Redirect the stdout and stderr
    stdout_path = os.path.join(args.logdir, f"funcx_worker_{args.worker_id}.stdout")
    stderr_path = os.path.join(args.logdir, f"funcx_worker_{args.worker_id}.stderr")
    with open(stdout_path, "w") as fo, open(stderr_path, "w") as fe:
        # Redirect the stdout
        old_stdout, old_stderr = sys.stdout, sys.stderr
        sys.stdout = fo
        sys.stderr = fe

        try:
            worker = Worker(
                args.worker_id,
                args.address,
                int(args.port),
                worker_type=args.type,
            )
            worker.start()
        finally:
            # Switch them back
            sys.stdout = old_stdout
            sys.stderr = old_stderr


if __name__ == "__main__":
    cli_run()
