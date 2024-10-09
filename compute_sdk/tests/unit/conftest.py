import json
import os
import random
import string
from unittest import mock

import pytest
import requests
from globus_compute_sdk.sdk.compute_dir import ensure_compute_dir
from globus_sdk import GlobusHTTPResponse


@pytest.fixture
def randomstring():
    def func(length=5, alphabet=string.ascii_letters):
        return "".join(random.choice(alphabet) for _ in range(length))

    return func


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
    env["GLOBUS_COMPUTE_USER_DIR"] = str(tmp_path / "home")
    with mock.patch.dict(os.environ, env):
        yield ensure_compute_dir().absolute()
