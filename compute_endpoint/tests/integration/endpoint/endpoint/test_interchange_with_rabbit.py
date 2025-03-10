from __future__ import annotations

import json
import logging
import multiprocessing
import time
import uuid
import warnings

import pika
import pytest
from globus_compute_common.messagepack import pack, unpack
from globus_compute_common.messagepack.message_types import EPStatusReport, Result, Task
from globus_compute_endpoint.endpoint.config import UserEndpointConfig
from globus_compute_endpoint.endpoint.interchange import EndpointInterchange
from tests.integration.endpoint.executors.mock_executors import MockExecutor
from tests.utils import try_for_timeout

_test_func_ids = [str(uuid.uuid4()) for i in range(3)]


@pytest.fixture
def run_interchange_process(
    get_standard_compute_client,
    setup_register_endpoint_response,
    tmp_path,
    request,  # Allows a custom config to be passed in if needed
    ep_uuid,
):
    """
    Start and stop a subprocess that executes the EndpointInterchange class.

    Yields a tuple of the interchange subprocess, (temporary) working directory,
    a random endpoint id, and the mocked registration info.
    """

    def run_it(reg_info: dict, endpoint_uuid, endpoint_dir):
        mock_exe = MockExecutor()
        mock_exe.endpoint_id = endpoint_uuid
        mock_exe.executor_exception = None
        mock_exe.get_status_report.return_value = EPStatusReport(
            endpoint_id=endpoint_uuid, global_state={}, task_statuses=[]
        )

        if hasattr(request, "param") and request.param:
            config = request.param
        else:
            config = UserEndpointConfig(executors=[])
        config.executors = [mock_exe]

        ix = EndpointInterchange(
            config=config,
            endpoint_id=endpoint_uuid,
            reg_info=reg_info,
            ep_info={},
            endpoint_dir=endpoint_dir,
            logdir=endpoint_dir,
        )

        ix.start()

    endpoint_name = "endpoint_foo"
    gcc = get_standard_compute_client()
    setup_register_endpoint_response(ep_uuid)
    reg_info = gcc.register_endpoint(endpoint_name, ep_uuid)
    assert isinstance(reg_info, dict), "Test setup verification"
    assert reg_info["endpoint_id"] == ep_uuid, "Test setup verification"
    assert "task_queue_info" in reg_info
    assert "result_queue_info" in reg_info
    assert "heartbeat_queue_info" in reg_info

    ix_proc = multiprocessing.Process(
        target=run_it, args=(reg_info, ep_uuid), kwargs={"endpoint_dir": tmp_path}
    )
    ix_proc.start()

    yield ix_proc, tmp_path, ep_uuid, reg_info

    if ix_proc.is_alive():
        ix_proc.terminate()
        try_for_timeout(lambda: not ix_proc.is_alive())

    rc = ix_proc.exitcode
    if rc is not None and rc != 0:
        warnings.warn(f"Interchange process exited with nonzero result code: {rc}")

    if rc is None:
        warnings.warn("Interchange process did not shut down cleanly - send SIGKILL")
        ix_proc.kill()
    ix_proc.join()
    ix_proc.close()


def test_epi_graceful_shutdown(run_interchange_process):
    ix_proc, tmp_path, ep_uuid, _reg_info = run_interchange_process
    time.sleep(2)  # simple test approach for now: assume it's up after 2s
    ix_proc.terminate()
    assert try_for_timeout(lambda: ix_proc.exitcode is not None), "Failed to shutdown"


def test_epi_forwards_tasks_and_results(
    run_interchange_process, pika_conn_params, randomstring
):
    """
    Verify the two main threads of kernel interest: that tasks are pulled from the
    appropriate queue, and results are put into appropriate queue with the correct
    routing_key.
    """
    ix_proc, tmp_path, ep_uuid, reg_info = run_interchange_process

    task_uuid = uuid.uuid4()
    task_msg = Task(task_id=task_uuid, task_buffer=randomstring())
    task_q, res_q = (reg_info["task_queue_info"], reg_info["result_queue_info"])
    res_q_name = res_q["queue"]
    task_q_name = task_q["queue"]
    task_exch = task_q["exchange"]
    with pika.BlockingConnection(pika_conn_params) as mq_conn:
        with mq_conn.channel() as chan:
            chan.queue_purge(task_q_name)
            chan.queue_purge(res_q_name)
            # publish our canary task
            chan.basic_publish(
                task_exch,
                task_q_name,
                pack(task_msg),
                properties=pika.BasicProperties(
                    content_type="application/json",
                    content_encoding="utf-8",
                    headers={
                        "function_uuid": "some_func",
                        "task_uuid": str(task_uuid),
                        "resource_specification": "null",
                    },
                ),
            )

            # then get (consume) our expected result
            result: Result | None = None
            for mframe, mprops, mbody in chan.consume(
                queue=res_q_name,
                inactivity_timeout=10,
            ):
                assert (mframe, mprops, mbody) != (None, None, None), "no timely result"
                result = unpack(mbody)
                break
    assert result.task_id == task_uuid
    assert result.data == task_msg.task_buffer


@pytest.mark.parametrize(
    "resource_specification",
    [
        "NO_HEADER",  # SPECIAL CASE: Missing header
        "null",
        None,
        '{"num_nodes": 2, "ranks_per_node": 2, "num_ranks": 4}',
    ],
)
def test_resource_specification(
    run_interchange_process, pika_conn_params, randomstring, resource_specification
):
    """
    Verify the two main threads of kernel interest: that tasks are pulled from the
    appropriate queue, and results are put into appropriate queue with the correct
    routing_key.
    """
    ix_proc, tmp_path, ep_uuid, reg_info = run_interchange_process

    task_uuid = uuid.uuid4()
    task_msg = Task(task_id=task_uuid, task_buffer=randomstring())
    task_q, res_q = reg_info["task_queue_info"], reg_info["result_queue_info"]
    res_q_name = res_q["queue"]
    task_q_name = task_q["queue"]
    task_exch = task_q["exchange"]
    with pika.BlockingConnection(pika_conn_params) as mq_conn:
        with mq_conn.channel() as chan:
            chan.queue_purge(task_q_name)
            chan.queue_purge(res_q_name)
            # publish our canary task
            header = {
                "function_uuid": "some_func",
                "task_uuid": str(task_uuid),
            }

            if resource_specification != "NO_HEADER":
                header["resource_specification"] = resource_specification
            chan.basic_publish(
                task_exch,
                task_q_name,
                pack(task_msg),
                properties=pika.BasicProperties(
                    content_type="application/json",
                    content_encoding="utf-8",
                    headers=header,
                ),
            )

            # then get (consume) our expected result
            result: Result | None = None
            for mframe, mprops, mbody in chan.consume(
                queue=res_q_name,
            ):
                assert (mframe, mprops, mbody) != (None, None, None), "no timely result"
                result = unpack(mbody)
                break
    assert result.task_id == task_uuid
    assert result.data == task_msg.task_buffer


def test_bad_resource_specification(
    run_interchange_process, pika_conn_params, randomstring
):
    """
    Verify that a result is returned for a task that carries
    a bad resource_spec, in this case we use {"BAD_KEY": ...}
    to trigger an exception in the MockExecutor
    """
    ix_proc, tmp_path, ep_uuid, reg_info = run_interchange_process

    task_uuid = uuid.uuid4()
    task_msg = Task(task_id=task_uuid, task_buffer=randomstring())
    task_q, res_q = reg_info["task_queue_info"], reg_info["result_queue_info"]
    res_q_name = res_q["queue"]
    task_q_name = task_q["queue"]
    task_exch = task_q["exchange"]

    # The MockExecutor raises an exception on receiving res_spec with BAD_KEY
    resource_specification = json.dumps({"BAD_KEY": "BAD_VALUE"})
    with pika.BlockingConnection(pika_conn_params) as mq_conn:
        with mq_conn.channel() as chan:
            x = chan.is_open
            logging.warning(f"Channel open : {x=}")
            chan.queue_purge(task_q_name)
            chan.queue_purge(res_q_name)
            # publish our canary task
            chan.basic_publish(
                task_exch,
                task_q_name,
                pack(task_msg),
                properties=pika.BasicProperties(
                    content_type="application/json",
                    content_encoding="utf-8",
                    headers={
                        "function_uuid": "some_func",
                        "task_uuid": str(task_uuid),
                        "resource_specification": resource_specification,
                    },
                ),
            )

            # then get (consume) our expected result
            result: Result | None = None
            for mframe, mprops, mbody in chan.consume(
                queue=res_q_name,
            ):
                assert (mframe, mprops, mbody) != (None, None, None), "no timely result"
                result = unpack(mbody)
                break
    assert result.task_id == task_uuid
    assert "Invalid Resource Specification Supplied: {'BAD_KEY'}" in result.data
    assert result.error_details.code == "RemoteExecutionError"
