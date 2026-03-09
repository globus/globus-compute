import json
import os
import random
import string
from unittest import mock

import pytest
import requests
from globus_compute_sdk.sdk.compute_dir import ensure_compute_dir
from globus_sdk import GlobusHTTPResponse


def randomstring_impl(length=5, alphabet=string.ascii_letters):
    return "".join(random.choice(alphabet) for _ in range(length))


@pytest.fixture
def randomstring():
    return randomstring_impl


@pytest.fixture
def mock_response():
    def response(resp_code, resp_json) -> GlobusHTTPResponse:
        resp = requests.Response()
        resp.status_code = resp_code
        resp.code = f"Mock {resp_code}"
        resp._content = json.dumps(resp_json).encode("utf-8")
        return GlobusHTTPResponse(resp, client=mock.Mock())

    yield response


@pytest.fixture
def run_in_tmp_dir(tmp_path):
    pwd = os.getcwd()
    os.chdir(tmp_path)
    yield tmp_path
    os.chdir(pwd)


@pytest.fixture
def mock_gc_home(tmp_path):
    env = os.environ.copy()
    env["GLOBUS_COMPUTE_USER_DIR"] = str(tmp_path / "home/mock_gc")
    with mock.patch.dict(os.environ, env):
        yield ensure_compute_dir().absolute()


@pytest.fixture
def mock_gc_home_other(tmp_path):
    def _mock_base_dir(base_dir: str):
        env = os.environ.copy()
        base_absolute_dir = str((tmp_path / base_dir).absolute())
        env["GLOBUS_COMPUTE_USER_DIR"] = base_absolute_dir
        with mock.patch.dict(os.environ, env):
            return ensure_compute_dir().absolute()

    yield _mock_base_dir


@pytest.fixture
def mock_worker_lost_task():
    return {
        "task_id": "259a07a4-f218-4ed2-8aa6-1234567890ab",
        "status": "failed",
        "completion_t": "1773080793.4360487",
        "exception": (
            "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n"
            "Traceback (most recent call last):\n"
            '  File "/home/ubuntu/.pyenv/versions/3.12.7/lib/python3.12/concurrent/futures/_base.py", line 401, in __get_result\n'  # noqa E501
            "    raise self._exception\n"
            '  File "/home/ubuntu/p3.12.7/lib/python3.12/site-packages/parsl/executors/high_throughput/executor.py", line 538, in _result_queue_worker\n'  # noqa E501
            "    s.reraise()\n"
            '  File "/home/ubuntu/p3.12.7/lib/python3.12/site-packages/parsl/app/errors.py", line 114, in reraise\n'  # noqa E501
            "    raise v\n"
            '  File "/home/ubuntu/p3.12.7/lib/python3.12/site-packages/parsl/executors/high_throughput/process_worker_pool.py", line 452, in worker_watchdog\n'  # noqa E501
            "    raise WorkerLost(worker_id, platform.node())\n"
            "    ^^^^^^^^^^^^^^^^^\n"
            "parsl.executors.high_throughput.errors.WorkerLost: Task failure due to loss of worker 0 on host ip-172-31-16-75\n"  # noqa E501
            "--------------------------------------------------------------------"
        ),
    }
