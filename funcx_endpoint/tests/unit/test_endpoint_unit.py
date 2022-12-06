import functools
import io
import pathlib
import random
import uuid
from collections import namedtuple
from unittest import mock

import pytest

from funcx_endpoint.endpoint import endpoint
from funcx_endpoint.endpoint.endpoint import Endpoint
from funcx_endpoint.endpoint.utils.config import Config


@pytest.fixture
def mock_ep_buf():
    buf = io.StringIO()
    # Endpoint.get_endpoint
    # ep = mocker.patch("funcx_endpoint.endpoint.endpoint.Endpoint.get_endpoints")
    Endpoint.get_endpoints = mock.Mock()
    Endpoint.get_endpoints.return_value = {}

    Endpoint.print_endpoint_table = functools.partial(
        Endpoint.print_endpoint_table, conf_dir="unused", ofile=buf
    )
    yield buf


def test_start_endpoint(mocker, fs, randomstring):
    mock_daemon = mocker.patch("funcx_endpoint.endpoint.endpoint.daemon")
    mock_epinterchange = mocker.patch(
        "funcx_endpoint.endpoint.endpoint.EndpointInterchange"
    )
    mock_funcxclient = mocker.patch("funcx_endpoint.endpoint.endpoint.FuncXClient")

    funcx_dir = pathlib.Path(endpoint._DEFAULT_FUNCX_DIR)
    ep = endpoint.Endpoint()

    (funcx_dir / ep.name).mkdir(parents=True, exist_ok=True)

    ep_id = str(uuid.uuid4())
    log_to_console = False
    no_color = True
    ep_conf = Config()

    mock_funcxclient.return_value.register_endpoint.return_value = {
        "endpoint_id": ep_id,
        "task_queue_info": {},
        "result_queue_info": {},
    }

    ep.start_endpoint(ep.name, ep_id, ep_conf, log_to_console, no_color)
    mock_epinterchange.assert_called()
    mock_daemon.DaemonContext.assert_called()


def test_list_endpoints_none_configured(mock_ep_buf):
    buf = mock_ep_buf
    Endpoint.print_endpoint_table()
    assert "No endpoints configured" in buf.getvalue()
    assert "Hint:" in buf.getvalue()
    assert "funcx-endpoint configure" in buf.getvalue()


def test_list_endpoints_no_id_yet(mock_ep_buf, randomstring):
    buf = mock_ep_buf
    expected_col_length = random.randint(2, 30)
    Endpoint.get_endpoints.return_value = {
        "default": {"status": randomstring(length=expected_col_length), "id": None}
    }
    Endpoint.print_endpoint_table()
    assert Endpoint.get_endpoints.return_value["default"]["status"] in buf.getvalue()
    assert "| Endpoint ID |" in buf.getvalue(), "Expecting column shrinks to size"


@pytest.mark.parametrize("term_size", ((30, 5), (50, 5), (67, 5), (72, 5), (120, 5)))
def test_list_endpoints_long_names_wrapped(
    mock_ep_buf, mocker, term_size, randomstring
):
    buf = mock_ep_buf
    tsize = namedtuple("terminal_size", ["columns", "lines"])(*term_size)
    mock_shutil = mocker.patch("funcx_endpoint.endpoint.endpoint.shutil")
    mock_shutil.get_terminal_size.return_value = tsize

    def rand_length_str(min_=2, max_=30):
        return randomstring(length=random.randint(min_, max_))

    expected_data = {
        rand_length_str(100, 110): {"status": rand_length_str(), "id": uuid.uuid4()},
        rand_length_str(100, 110): {"status": rand_length_str(), "id": uuid.uuid4()},
        rand_length_str(100, 110): {"status": rand_length_str(), "id": uuid.uuid4()},
        rand_length_str(100, 110): {"status": rand_length_str(), "id": None},
        rand_length_str(100, 110): {"status": rand_length_str(), "id": uuid.uuid4()},
    }
    Endpoint.get_endpoints.return_value = expected_data

    Endpoint.print_endpoint_table()

    for ep_name, ep in expected_data.items():
        assert ep["status"] in buf.getvalue(), "expected no wrapping of status"
        assert str(ep["id"]) in buf.getvalue(), "expected no wrapping of id"
        assert ep_name not in buf.getvalue(), "expected only name column is wrapped"


@pytest.mark.parametrize(
    "pid_info",
    [
        [False, None, False, False],
        [True, "", True, False],
        [True, "123", True, False],
    ],
)
def test_pid_file_check(pid_info, fs):
    has_file, pid_content, should_exist, should_active = pid_info

    pid_path = "sample_daemon.pid"
    if has_file:
        with open(pid_path, "w") as f:
            f.write(pid_content)

    pid_status = Endpoint.check_pidfile(pid_path)
    assert should_exist == pid_status["exists"]
    assert should_active == pid_status["active"]
