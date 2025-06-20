import fcntl
import io
import json
import logging
import os
import textwrap
import threading
from unittest import mock

import pytest
from globus_compute_endpoint.endpoint.config import ManagerEndpointConfig
from globus_compute_endpoint.endpoint.endpoint_manager import (
    EndpointManager,
    MappedPosixIdentity,
)
from tests.utils import try_assert

_MOCK_BASE = "globus_compute_endpoint.endpoint.endpoint_manager."
_GOOD_UNPRIVILEGED_EC = 84


@pytest.fixture(autouse=True, scope="module")
def tell_fakefs_to_goaway():
    """
    Absolute hack, but I haven't yet figured out how to truly disable or remove
    the PyFakeFS machinery that other test modules utilize.  Unfortunately, the
    code under test in this module is low-level enough (pipe/pipe2) that pyfakefs
    can't do the job, so since the tendrils from previous tests are still in the
    sys.modules and elsewhere, use this context manager.

    Consider this a challenge, dear reader: disable this fixture and get the tests
    to pass.  Go!
    """
    from pyfakefs.fake_os import use_original_os

    with use_original_os():
        yield


@pytest.fixture
def mock_log():
    with mock.patch(f"{_MOCK_BASE}log", spec=logging.Logger) as m:
        m.getEffectiveLevel.return_value = logging.DEBUG
        yield m


@pytest.fixture
def mock_close_fds():
    with mock.patch(f"{_MOCK_BASE}close_all_fds") as m:
        yield m


def conf_tmpl():
    return textwrap.dedent(
        """
        heartbeat_period: 1
        idle_heartbeats_soft: 1
        idle_heartbeats_hard: 2
        engine:
            type: ThreadPoolEngine
            max_workers: 1
    """
    ).strip()


@pytest.fixture
def conf(tmp_path):
    (tmp_path / "user_config_template.yaml.j2").write_text(conf_tmpl())
    mec = ManagerEndpointConfig(multi_user=True, high_assurance=True)
    mec.audit_log_path = tmp_path / "audit.log"
    test_environ = {
        "HOME": str(tmp_path),
    }
    with mock.patch.dict(os.environ, test_environ):
        yield mec


@pytest.fixture
def reg_info(ep_uuid):
    yield {
        "endpoint_id": ep_uuid,
        "command_queue_info": {"connection_url": "", "queue": ""},
        "result_queue_info": {
            "connection_url": "",
            "queue": "",
            "queue_publish_kwargs": {},
        },
        "heartbeat_queue_info": {
            "connection_url": "",
            "queue": "",
            "queue_publish_kwargs": {},
        },
    }


@pytest.fixture
def mock_os():
    with mock.patch(f"{_MOCK_BASE}os") as m:
        m.O_DIRECT = os.O_DIRECT
        m.fork.return_value = 0
        m.getuid = os.getuid
        m.getgid = os.getgid
        m.getpid = os.getpid
        m.getppid = os.getppid
        m.read = os.read

        m.pipe.return_value = 40, 41
        m.dup2.side_effect = (0, 1, 2, AssertionError("dup2: unexpected?"))
        m.open.side_effect = (4, 5, AssertionError("open: unexpected?"))

        with mock.patch.object(fcntl, "fcntl", return_value=8192):
            yield m


def test_audit_log_write(tmp_path, conf, ep_uuid, reg_info):
    em = EndpointManager(tmp_path, ep_uuid, conf, reg_info)

    r, w = os.pipe2(os.O_DIRECT)
    os.write(w, b"Test1")
    os.close(w)

    r_info = {"uid": 10001, "endpoint_id": "ep1", "pid": 101}
    em._audit_pipes[r] = r_info
    buf = io.BytesIO()
    em._audit_log_write(r, buf)
    os.close(r)

    rec = buf.getvalue().decode()
    assert f"uid={r_info['uid']}" in rec
    assert f"pid={r_info['pid']}" in rec
    assert f"uep={r_info['endpoint_id']}" in rec
    assert rec.endswith("Test1\n")


def test_audit_log_write_unknown_fd(mock_log, tmp_path, conf, ep_uuid, reg_info):
    em = EndpointManager(tmp_path, ep_uuid, conf, reg_info)

    assert not em._audit_pipes, "Verify test setup"
    r, w = os.pipe2(os.O_DIRECT)
    os.write(w, b"Test1")
    os.close(w)
    em._audit_log_write(r, None)
    with pytest.raises(OSError) as pyt_e:
        os.close(r)

    assert "Bad file descriptor" in str(pyt_e), "Expect failure to write closes pipe"


def test_audit_log_write_close_on_closed_pipe(
    mock_log, tmp_path, conf, ep_uuid, reg_info
):
    em = EndpointManager(tmp_path, ep_uuid, conf, reg_info)

    r, w = os.pipe2(os.O_DIRECT)
    os.write(w, b"Test")
    os.close(w)

    em._audit_pipes[r] = {"uid": 10001, "endpoint_id": "ep1", "pid": 101}

    buf = io.BytesIO()
    em._audit_log_write(r, buf)
    assert buf.getvalue().decode().endswith("Test\n")

    em._audit_log_write(r, buf)
    with pytest.raises(OSError) as pyt_e:
        os.close(r)

    assert "Bad file descriptor" in str(pyt_e), "Expect failure to write closes pipe"


def test_audit_log_shutsdown_on_write_error(
    mock_log, tmp_path, conf, ep_uuid, reg_info
):
    em = EndpointManager(tmp_path, ep_uuid, conf, reg_info)

    r, w = os.pipe2(os.O_DIRECT)
    os.write(w, b"Test1")
    os.close(w)
    r_info = {"uid": 10001, "endpoint_id": "ep1", "pid": 101}
    em._audit_pipes[r] = r_info
    assert em._time_to_stop is False

    em._audit_log_write(r, None)
    os.close(r)
    (log,), _ = mock_log.error.call_args
    assert "has no attribute 'write'" in log, "verify *test*-created failure"
    assert em._time_to_stop, "Key outcome"


def test_audit_log_shutsdown_on_general_error(
    tmp_path, ep_uuid, conf, reg_info, mock_os, randomstring
):
    em = EndpointManager(tmp_path, ep_uuid, conf, reg_info)

    exc_text = randomstring()
    with mock.patch(f"{_MOCK_BASE}open", side_effect=MemoryError(exc_text)):
        with pytest.raises(MemoryError) as pyt_e:
            em._audit_log_impl()
    assert exc_text in str(pyt_e.value), "Verify that test induced failure"
    assert em._time_to_stop is True, "Expect shutdown, no matter the error"


def test_audit_log_pipe_hookup(
    mock_log, tmp_path, ep_uuid, conf, reg_info, mock_os, mock_close_fds
):
    em = EndpointManager(tmp_path, ep_uuid, conf, reg_info)

    m = mock.Mock()
    mock_os.fdopen.return_value.__enter__.return_value = m
    mock_os.pipe2.side_effect = os.pipe2  # need actual pipes ...

    mpi = MappedPosixIdentity(em._mu_user, [], ep_uuid)
    kwargs = {
        "name": "some_endpoint_name",
        "amqp_creds": {"endpoint_id": ep_uuid},
        "user_opts": {"heartbeat": 10},
    }

    with pytest.raises(SystemExit) as pyt_e:
        em.cmd_start_endpoint(mpi, None, kwargs)

    assert pyt_e.value.code == _GOOD_UNPRIVILEGED_EC, "Q&D: verify we exec'ed'"

    (stdin_str,), _ = m.write.call_args
    stdin_data = json.loads(stdin_str)
    audit_w = stdin_data["audit_fd"]
    os.write(audit_w, b"FROM CHILD, THROUGH KERNEL PIPE")

    t = threading.Thread(target=em._audit_log_impl, daemon=True)
    t.start()
    try_assert(conf.audit_log_path.exists)
    try_assert(lambda: bool(conf.audit_log_path.read_text()))
    em._audit_log_handler_stop = True
    os.close(audit_w)
    t.join()

    rec = conf.audit_log_path.read_text()
    assert "FROM CHILD" in rec
