import concurrent.futures
import queue
import random
import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

import pytest
from globus_compute_common import messagepack
from globus_compute_endpoint.engines import GlobusComputeEngine
from parsl.executors.high_throughput.interchange import ManagerLost
from parsl.providers import LocalProvider
from tests.utils import slow_double


class MockHTEX:
    """This class matches Threadpool to HTEX's interface and mocks repeated
    ManagerLost errors"""

    def __init__(self, fail_count=1):
        self.fail_count = fail_count
        self.ex = ThreadPoolExecutor()

    def submit(self, func, _resource_spec, *args, **kwargs):
        """Inject failures and match Threadpool and HTEX submit signatures"""
        if self.fail_count > 0:
            self.fail_count -= 1
            future = concurrent.futures.Future()
            future.set_exception(ManagerLost(b"DEAD", "Faking Manager death!"))
        else:
            future = self.ex.submit(func, *args, **kwargs)
        return future

    def shutdown(self):
        return self.ex.shutdown()


@pytest.fixture
def mock_gce(tmp_path):

    executor = MockHTEX()

    executor.launch_cmd = ""
    scripts_dir = str(tmp_path / "submit_scripts")
    with mock.patch.object(GlobusComputeEngine, "_ExecutorClass", MockHTEX) as mock_Ex:
        mock_Ex.__name__ = MockHTEX.__name__
        engine = GlobusComputeEngine(
            executor=executor,
            working_dir=scripts_dir,
            provider=LocalProvider(min_blocks=0, max_blocks=0, init_blocks=0),
        )
        engine.results_passthrough = queue.Queue()
        yield engine


def test_success_after_1_fail(mock_gce, serde, ez_pack_task):
    engine = mock_gce
    engine.max_retries_on_system_failure = 2
    q = engine.results_passthrough
    task_id = uuid.uuid1()
    num = random.randint(1, 10000)
    task_bytes = ez_pack_task(slow_double, num, 0.2)

    # Set the failure count on the mock executor to force failure
    engine.executor.fail_count = 1
    engine.submit(task_id, task_bytes, resource_specification={})

    packed_result = q.get()
    assert isinstance(packed_result, dict)
    result = messagepack.unpack(packed_result["message"])

    assert result.task_id == task_id
    assert serde.deserialize(result.data) == 2 * num


def test_repeated_fail(mock_gce, ez_pack_task):
    fail_count = 2
    engine = mock_gce
    engine.max_retries_on_system_failure = fail_count
    q = engine.results_passthrough
    task_id = uuid.uuid1()

    # Set executor to continue failures beyond retry limit
    engine.executor.fail_count = fail_count + 1

    task_bytes = ez_pack_task(slow_double, 5)

    engine.submit(task_id, task_bytes, resource_specification={})

    packed_result_q = q.get()
    result = messagepack.unpack(packed_result_q["message"])
    assert isinstance(result, messagepack.message_types.Result)
    assert result.task_id == task_id
    assert result.error_details
    assert "ManagerLost" in result.data
    count = result.data.count("Traceback from attempt")
    assert count == fail_count + 1, "Got incorrect # of failure reports"
    assert "final attempt" in result.data


def test_default_retries_is_0():
    engine = GlobusComputeEngine(address="127.0.0.1")
    assert engine.max_retries_on_system_failure == 0, "Users must knowingly opt-in"
