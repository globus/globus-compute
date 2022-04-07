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


def run_ix_process(reg_info, endpoint_uuid):
    results_ack_handler = ResultsAckHandler(
        endpoint_dir="../../test_ep_with_mock_service"
    )
    ix = EndpointInterchange(
        config=config,
        endpoint_id=endpoint_uuid,
        results_ack_handler=results_ack_handler,
        reg_info=reg_info,
    )

    logging.warning("IX created, starting")
    ix.start()
    logging.warning("Done")


def test_endpoint_interchange_against_rabbitmq(endpoint_uuid):
    """This test registers the endpoint against local RabbitMQ, and
    uses mock tasks
    """
    logging.warning(f"Current proc pid: {os.getpid()}")

    # Use register_endpoint to get connection params
    endpoint_name = "endpoint_foo"
    fxc = funcx.FuncXClient(use_offprocess_checker=False)
    reg_info = register_endpoint(
        fxc,
        endpoint_uuid=endpoint_uuid,
        endpoint_dir=os.getcwd(),
        endpoint_name=endpoint_name,
    )
    assert isinstance(reg_info, pika.URLParameters)

    # Start the service side components:
    # Connect the TaskQueuePublisher and submit some mock tasks
    task_q_out = TaskQueuePublisher(
        endpoint_uuid=endpoint_uuid, pika_conn_params=reg_info
    )
    task_q_out.connect()
    task_q_out._channel.queue_purge(task_q_out.queue_name)
    logging.warning(f"Publishing task to {endpoint_uuid}")

    result_q_in = multiprocessing.Queue()
    result_sub_proc = ResultQueueSubscriber(
        pika_conn_params=reg_info,
        external_queue=result_q_in,
    )
    result_sub_proc.start()

    # Make a Task and publish it
    task = make_mock_task()
    task_q_out.publish(task.pack())

    # Create the endpoint
    ix_proc = multiprocessing.Process(
        target=run_ix_process,
        args=(
            reg_info,
            endpoint_uuid,
        ),
    )
    ix_proc.start()
    time.sleep(2)

    for _i in range(10):
        from_ep, b_message = result_q_in.get()
        # logging.warning(f"Got message: {message} from {from_ep}")

        assert from_ep == endpoint_uuid
        try:
            message = pickle.loads(b_message)
        except Exception:
            logging.warning(f"Unknown message: {b_message} from {from_ep}")
            continue

        if isinstance(message, EPStatusReport):
            continue

        if "result" in message or "exception" in message:
            logging.warning(f"Received result/exception for {message['task_id']}")
            assert message["task_id"] == task.task_id
            break

    logging.warning("Exiting...")
    result_sub_proc.terminate()
    task_q_out.close()
    ix_proc.terminate()
