import logging
import multiprocessing
import os
import pickle
import time

import pika
from parsl.providers import LocalProvider

import funcx
from funcx_endpoint.endpoint.interchange import EndpointInterchange
from funcx_endpoint.endpoint.rabbit_mq import ResultQueueSubscriber, TaskQueuePublisher
from funcx_endpoint.endpoint.register_endpoint import register_endpoint
from funcx_endpoint.endpoint.results_ack import ResultsAckHandler
from funcx_endpoint.endpoint.utils.config import Config
from funcx_endpoint.executors import HighThroughputExecutor
from funcx_endpoint.executors.high_throughput.messages import EPStatusReport, Task

config = Config(
    executors=[
        HighThroughputExecutor(
            provider=LocalProvider(
                init_blocks=1,
                min_blocks=0,
                max_blocks=1,
            ),
        )
    ],
    funcx_service_address="https://api2.funcx.org/v2",
)

ENDPOINT_UUID = "9d8f5fcc-8d70-4d8e-a027-510ed6084b2e"

LOG_FORMAT = "%(levelname) -10s %(asctime)s %(name) -20s %(lineno) -5d: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def make_mock_task():
    task_buf = (
        b"\x05TID=16a3b7d0-1c7f-4701-b3d5-dc22e8819f79;CID=RAW;"
        b"85\n04\ngANYBgAAAGRvdWJsZXEAWCAAAABkZWYgZG91YmxlKHgpO"
        b"gogICAgcmV0dXJuIHggKiAyCnEBhnEC\nLg==\n16\n00\ngANLAI"
        b"VxAC4=\n12\n00\ngAN9cQAu\n"
    )
    task = Task(
        task_id="16a3b7d0-1c7f-4701-b3d5-dc22e8819f79",
        container_id="RAW",
        task_buffer=task_buf,
    )
    return task


def run_ix_process(reg_info):
    results_ack_handler = ResultsAckHandler(endpoint_dir=".")
    ix = EndpointInterchange(
        config=config,
        endpoint_id=ENDPOINT_UUID,
        results_ack_handler=results_ack_handler,
        reg_info=reg_info,
    )

    logger.warning("IX created, starting")
    ix.start()
    logger.warning("Done")


def test_endpoint_interchange_against_rabbitmq():
    """This test registers the endpoint against local RabbitMQ, and
    uses mock tasks
    """

    # Use register_endpoint to get connection params
    endpoint_name = "endpoint_foo"
    fxc = funcx.FuncXClient()
    reg_info = register_endpoint(
        fxc,
        endpoint_uuid=ENDPOINT_UUID,
        endpoint_dir=os.getcwd(),
        endpoint_name=endpoint_name,
    )
    assert isinstance(reg_info, pika.URLParameters)

    # Start the service side components:
    # Connect the TaskQueuePublisher and submit some mock tasks
    task_q_out = TaskQueuePublisher(
        endpoint_uuid=ENDPOINT_UUID, pika_conn_params=reg_info
    )
    task_q_out.connect()
    logger.warning(f"Publishing task to {ENDPOINT_UUID}")

    result_q_in = multiprocessing.Queue()
    kill_event = multiprocessing.Event()
    result_pub_proc = ResultQueueSubscriber(
        pika_conn_params=reg_info, external_queue=result_q_in, kill_event=kill_event
    )
    result_pub_proc.start()

    # Make a Task and publish it
    task = make_mock_task()
    task_q_out.publish(task.pack())

    # Create the endpoint
    ix_proc = multiprocessing.Process(target=run_ix_process, args=(reg_info,))
    ix_proc.start()

    time.sleep(2)
    for _i in range(10):
        from_ep, b_message = result_q_in.get()
        # logger.warning(f"Got message: {message} from {from_ep}")

        assert from_ep == ENDPOINT_UUID
        try:
            message = pickle.loads(b_message)
        except Exception:
            logger.warning(f"Unknown message: {b_message} from {from_ep}")
            continue

        if isinstance(message, EPStatusReport):
            logger.warning(f"Got EPStatusMessage: {message}")
            continue

        if "result" in message or "exception" in message:
            logger.warning(f"Received result/exception for {message['task_id']}")
            assert message["task_id"] == task.task_id
            logger.warning(f"Message : {message}")
            break

    logger.warning("Exiting...")
    result_pub_proc.terminate()
    task_q_out.close()
    ix_proc.terminate()
    ix_proc.join()
