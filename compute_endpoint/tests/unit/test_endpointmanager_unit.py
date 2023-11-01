import fcntl
import getpass
import io
import json
import os
import pathlib
import pwd
import queue
import random
import re
import resource
import shlex
import signal
import sys
import time
import typing as t
import uuid
from collections import namedtuple
from contextlib import redirect_stdout
from http import HTTPStatus
from unittest import mock

import jinja2
import jsonschema
import pika
import pytest as pytest
import responses
import yaml
from globus_compute_endpoint.endpoint.config import Config
from globus_compute_endpoint.endpoint.config.utils import (
    _validate_user_opts,
    load_user_config_schema,
    render_config_user_template,
)
from globus_compute_endpoint.endpoint.endpoint import Endpoint
from globus_compute_endpoint.endpoint.utils import _redact_url_creds
from globus_sdk import GlobusAPIError, NetworkError
from pytest_mock import MockFixture

try:
    import pyprctl
except AttributeError:
    pytest.skip(allow_module_level=True)
else:
    # these imports also import pyprctl later
    from globus_compute_endpoint.endpoint.endpoint_manager import (
        EndpointManager,
        InvalidUserError,
    )


_MOCK_BASE = "globus_compute_endpoint.endpoint.endpoint_manager."


@pytest.fixture
def conf_dir(fs):
    conf_dir = pathlib.Path("/some/path/mock_endpoint")
    conf_dir.mkdir(parents=True, exist_ok=True)
    yield conf_dir


@pytest.fixture
def mock_conf():
    yield Config(executors=[])


@pytest.fixture
def user_conf_template(conf_dir):
    template = Endpoint.user_config_template_path(conf_dir)
    template.write_text(
        """
heartbeat_period: {{ heartbeat }}
engine:
    type: HighThroughputEngine
    provider:
        type: LocalProvider
        init_blocks: 1
        min_blocks: 0
        max_blocks: 1
        """
    )


@pytest.fixture(autouse=True)
def mock_setproctitle(mocker, randomstring):
    orig_proc_title = randomstring()
    mock_spt = mocker.patch(f"{_MOCK_BASE}setproctitle")
    mock_spt.getproctitle.return_value = orig_proc_title
    yield mock_spt, orig_proc_title


@pytest.fixture
def mock_client(mocker):
    ep_uuid = str(uuid.uuid1())
    mock_gcc = mocker.Mock()
    mock_gcc.register_endpoint.return_value = {
        "endpoint_id": ep_uuid,
        "command_queue_info": {"connection_url": "", "queue": ""},
    }
    mocker.patch("globus_compute_sdk.Client", return_value=mock_gcc)
    yield ep_uuid, mock_gcc


@pytest.fixture
def epmanager(mocker, conf_dir, mock_conf, mock_client):
    ep_uuid, mock_gcc = mock_client

    # Needed to mock the pipe buffer size
    mocker.patch.object(fcntl, "fcntl", return_value=512)

    em = EndpointManager(conf_dir, ep_uuid, mock_conf)
    em._command = mocker.Mock()

    yield conf_dir, mock_conf, mock_client, em


@pytest.fixture
def register_endpoint_failure_response():
    def create_response(status_code: int = 200, err_msg: str = "some error msg"):
        responses.add(
            method=responses.POST,
            url="https://compute.api.globus.org/v2/endpoints",
            headers={"Content-Type": "application/json"},
            json={"error": err_msg},
            status=status_code,
        )

    return create_response


@pytest.fixture
def successful_exec(mocker, epmanager, user_conf_template, fs):
    # fs (pyfakefs) not used directly in this fixture, but is intentionally
    # utilized in epmanager -> conf_dir.  It is *assumed* in this fixture,
    # however (e.g., local_user_lookup.json), so make it an explicit detail.
    mock_os = mocker.patch(f"{_MOCK_BASE}os")
    conf_dir, mock_conf, mock_client, em = epmanager

    mock_os.fork.return_value = 0
    mock_os.pipe.return_value = 40, 41
    mock_os.dup2.side_effect = [0, 1, 2]
    mock_os.open.return_value = 4

    with open("local_user_lookup.json", "w") as f:
        json.dump({"a": getpass.getuser()}, f)

    props = pika.BasicProperties(
        content_type="application/json",
        content_encoding="utf-8",
        timestamp=round(time.time()),
        expiration="10000",
    )

    pld = {
        "globus_uuid": "a",
        "globus_username": "a",
        "command": "cmd_start_endpoint",
        "kwargs": {"name": "some_ep_name", "user_opts": {"heartbeat": 10}},
    }
    queue_item = (1, props, json.dumps(pld).encode())

    em._command_queue = mocker.Mock()
    em._command_stop_event.set()
    em._command_queue.get.side_effect = [queue_item, queue.Empty()]

    yield mock_os, conf_dir, mock_conf, mock_client, em


@pytest.mark.parametrize("env", [None, "blar", "local", "production"])
def test_sets_process_title(
    randomstring, conf_dir, mock_conf, mock_client, mock_setproctitle, env
):
    mock_spt, orig_proc_title = mock_setproctitle

    ep_uuid, mock_gcc = mock_client
    mock_conf.environment = env

    EndpointManager(conf_dir, ep_uuid, mock_conf)
    assert mock_spt.setproctitle.called, "Sanity check"

    a, *_ = mock_spt.setproctitle.call_args
    assert a[0].startswith(
        "Globus Compute Endpoint"
    ), "Expect easily identifiable process name"
    assert "*(" in a[0], "Expected asterisk as subtle clue of 'multi-user'"
    assert f"{ep_uuid}, {conf_dir.name}" in a[0], "Can find process by conf"

    if env:
        assert f" - {env}" in a[0], "Expected environment name in title"
    else:
        assert " - " not in a[0], "Default is not 'do not show env' for prod"
    assert a[0].endswith(f"[{orig_proc_title}]"), "Save original cmdline for debugging"


@responses.activate
@pytest.mark.parametrize(
    "exit_code,status_code",
    (
        (os.EX_UNAVAILABLE, HTTPStatus.CONFLICT),
        (os.EX_UNAVAILABLE, HTTPStatus.LOCKED),
        (os.EX_UNAVAILABLE, HTTPStatus.NOT_FOUND),
        (os.EX_DATAERR, HTTPStatus.BAD_REQUEST),
        (os.EX_DATAERR, HTTPStatus.UNPROCESSABLE_ENTITY),
        ("Error", 418),  # IM_A_TEAPOT
    ),
)
def test_gracefully_exits_if_registration_blocked(
    mocker,
    register_endpoint_failure_response,
    conf_dir,
    mock_conf,
    endpoint_uuid,
    randomstring,
    get_standard_compute_client,
    exit_code,
    status_code,
):
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    mock_gcc = get_standard_compute_client()
    mocker.patch("globus_compute_sdk.Client", return_value=mock_gcc)

    some_err = randomstring()
    register_endpoint_failure_response(status_code, some_err)

    f = io.StringIO()
    with redirect_stdout(f):
        with pytest.raises((GlobusAPIError, SystemExit)) as pyexc:
            EndpointManager(conf_dir, endpoint_uuid, mock_conf)
        stdout_msg = f.getvalue()

    assert mock_log.warning.called
    a, *_ = mock_log.warning.call_args
    assert some_err in str(a), "Expected upstream response still shared"

    assert some_err in stdout_msg, f"Expecting error message in stdout ({stdout_msg})"
    assert pyexc.value.code == exit_code, "Expecting meaningful exit code"

    if exit_code == "Error":
        # The other route tests SystemExit; nominally this route is an unhandled
        # traceback -- good.  We should _not_ blanket hide all exceptions.
        assert pyexc.value.http_status == status_code


def test_sends_metadata_during_registration(conf_dir, mock_conf, mock_client):
    ep_uuid, mock_gcc = mock_client
    mock_conf.multi_user = True
    EndpointManager(conf_dir, ep_uuid, mock_conf)

    assert mock_gcc.register_endpoint.called
    _a, k = mock_gcc.register_endpoint.call_args
    for key in (
        "endpoint_version",
        "hostname",
        "local_user",
        "config",
        "user_config_schema",
    ):
        assert key in k["metadata"], "Expected minimal metadata"

    for key in (
        "type",
        "multi_user",
        "stdout",
        "stderr",
        "environment",
        "funcx_service_address",
    ):
        assert key in k["metadata"]["config"]

    assert k["metadata"]["config"]["multi_user"] is True


def test_handles_network_error_scriptably(
    mocker,
    conf_dir,
    mock_conf,
    endpoint_uuid,
    randomstring,
):
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    some_err = randomstring()
    mocker.patch(
        "globus_compute_sdk.Client",
        side_effect=NetworkError(some_err, Exception()),
    )

    with pytest.raises(SystemExit) as pyexc:
        EndpointManager(conf_dir, endpoint_uuid, mock_conf)

    assert pyexc.value.code == os.EX_TEMPFAIL, "Expecting meaningful exit code"
    assert mock_log.exception.called, "Expected usable traceback"
    assert mock_log.critical.called
    a = mock_log.critical.call_args[0][0]
    assert "Network failure" in a
    assert some_err in a


def test_mismatched_id_gracefully_exits(
    mocker, randomstring, conf_dir, mock_conf, mock_client
):
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    wrong_uuid, mock_gcc = mock_client
    ep_uuid = str(uuid.uuid4())
    assert wrong_uuid != ep_uuid, "Verify test setup"

    with pytest.raises(SystemExit) as pyexc:
        EndpointManager(conf_dir, ep_uuid, mock_conf)

    assert pyexc.value.code == os.EX_SOFTWARE, "Expected meaningful exit code"
    assert mock_log.error.called
    a = mock_log.error.call_args[0][0]
    assert "mismatched endpoint" in a
    assert f"Expected: {ep_uuid}" in a
    assert f"received: {wrong_uuid}" in a


@pytest.mark.parametrize(
    "received_data",
    (
        [False, {"command_queue_info": {"connection_url": ""}}],
        [False, {"command_queue_info": {"queue": ""}}],
        [False, {"typo-ed_cqi": {"connection_url": "", "queue": ""}}],
        [True, {"command_queue_info": {"connection_url": "", "queue": ""}}],
    ),
)
def test_handles_invalid_reg_info(
    mocker, randomstring, conf_dir, mock_conf, mock_client, received_data
):
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    ep_uuid, mock_gcc = mock_client
    received_data[1]["endpoint_id"] = ep_uuid
    should_succeed, mock_gcc.register_endpoint.return_value = received_data

    if not should_succeed:
        with pytest.raises(SystemExit) as pyexc:
            EndpointManager(conf_dir, ep_uuid, mock_conf)
            assert pyexc.value.code == os.EX_DATAERR, "Expected meaningful exit code"
            assert mock_log.error.called
            a = mock_log.error.call_args[0][0]
            assert "Invalid or unexpected" in a
    else:
        # "null" test
        EndpointManager(conf_dir, ep_uuid, mock_conf)


def test_records_user_ep_as_running(successful_exec):
    mock_os, *_, em = successful_exec
    mock_os.fork.return_value = 1

    em._event_loop()

    uep_rec = em._children.pop(1)
    assert uep_rec.ep_name == "some_ep_name"


def test_caches_start_cmd_args_if_ep_already_running(successful_exec, mocker):
    *_, em = successful_exec
    child_pid = random.randrange(1, 32768 + 1)
    mock_uep = mocker.MagicMock()
    mock_uep.ep_name = "some_ep_name"
    em._children[child_pid] = mock_uep

    em._event_loop()

    assert child_pid in em._children
    cached_args = em._cached_cmd_start_args.pop(child_pid)
    assert cached_args is not None
    urec, args, kwargs = cached_args
    assert urec == pwd.getpwnam(getpass.getuser())
    assert args == []
    assert kwargs == {"name": "some_ep_name", "user_opts": {"heartbeat": 10}}


def test_writes_endpoint_uuid(epmanager):
    conf_dir, _mock_conf, mock_client, _em = epmanager
    _ep_uuid, mock_gcc = mock_client

    returned_uuid = mock_gcc.register_endpoint.return_value["endpoint_id"]

    ep_json_path = conf_dir / "endpoint.json"
    assert ep_json_path.exists()

    ep_data = json.loads(ep_json_path.read_text())
    assert ep_data["endpoint_id"] == returned_uuid


def test_log_contains_sentinel_lines(mocker, epmanager, noop, reset_signals):
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    conf_dir, mock_conf, mock_client, em = epmanager

    mocker.patch(f"{_MOCK_BASE}os")
    em._event_loop = noop
    em.start()

    uuid_pat = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    beg_re_sentinel = re.compile(r"\n\n=+ Endpoint Manager begins: ")
    beg_re_uuid = re.compile(rf"\n\n=+ Endpoint Manager begins: {uuid_pat}\n")
    end_re_sentinel = re.compile(r"\n-+ Endpoint Manager ends: ")
    end_re_uuid = re.compile(rf"\n-+ Endpoint Manager ends: {uuid_pat}\n\n")
    log_str = "\n".join(a[0] for a, _ in mock_log.info.call_args_list)
    assert log_str.startswith("\n\n"), "Expect visual separation for log trawlers"
    assert beg_re_sentinel.search(log_str) is not None, "Expected visual begin sentinel"
    assert beg_re_uuid.search(log_str) is not None, "Expected begin sentinel has EP id"
    assert "\nShutdown complete.\n" in log_str
    assert end_re_sentinel.search(log_str) is not None, "Expected visual end sentinel"
    assert end_re_uuid.search(log_str) is not None, "Expected end sentinel has EP id"


def test_title_changes_for_shutdown(
    mocker, epmanager, noop, mock_setproctitle, reset_signals
):
    conf_dir, mock_conf, mock_client, em = epmanager
    mock_spt, orig_proc_title = mock_setproctitle

    em._event_loop = noop
    mocker.patch(f"{_MOCK_BASE}os")

    mock_spt.reset_mock()
    assert not mock_spt.setproctitle.called, "Verify test setup"
    em.start()

    assert mock_spt.setproctitle.called
    a = mock_spt.setproctitle.call_args[0][0]
    assert a.startswith("[shutdown in progress]"), "Let admin know action in progress"
    assert a.endswith(orig_proc_title)


def test_children_signaled_at_shutdown(
    mocker, epmanager, randomstring, noop, reset_signals
):
    conf_dir, mock_conf, mock_client, em = epmanager

    em._event_loop = mocker.Mock()
    em.wait_for_children = noop
    mock_os = mocker.patch(f"{_MOCK_BASE}os")
    mock_time = mocker.patch(f"{_MOCK_BASE}time")
    mock_time.time.side_effect = [0, 10, 20, 30]  # don't _actually_ wait.
    mock_os.getuid.side_effect = ["us"]  # fail if called more than once; intentional
    mock_os.getgid.side_effect = ["us"]  # fail if called more than once; intentional
    mock_os.getpgid = lambda pid: pid

    expected = []
    for _ in range(random.randrange(0, 10)):
        uid, gid, pid = tuple(random.randint(1, 2**30) for _ in range(3))
        uname = randomstring()
        expected.append((uid, gid, uname, "some process command line"))
        mock_rec = mocker.MagicMock()
        mock_rec.uid, mock_rec.gid = uid, gid
        em._children[pid] = mock_rec

    gid_expected_calls = (
        a
        for b in zip(
            ((gid, gid, -1) for _uid, gid, *_ in expected),
            [("us", "us", -1)] * len(expected),  # return to root gid after signal
        )
        for a in b
    )
    uid_expected_calls = (
        a
        for b in zip(
            ((uid, uid, -1) for uid, _gid, *_ in expected),
            [("us", "us", -1)] * len(expected),  # return to root uid after signal
        )
        for a in b
    )

    # test that SIGTERM, *then* SIGKILL sent
    killpg_expected_calls = [(pid, signal.SIGTERM) for pid in em._children]
    killpg_expected_calls.extend((pid, signal.SIGKILL) for pid in em._children)

    em.start()
    assert em._event_loop.called, "Verify test setup"

    resgid = mock_os.setresgid.call_args_list
    resuid = mock_os.setresuid.call_args_list
    killpg = mock_os.killpg.call_args_list[1:]

    for setgid_call, exp_args in zip(resgid, gid_expected_calls):
        assert setgid_call[0] == exp_args, "Signals only sent by _same_ user, NOT root"
    for setuid_call, exp_args in zip(resuid, uid_expected_calls):
        assert setuid_call[0] == exp_args, "Signals only sent by _same_ user, NOT root"
    for killpg_call, exp_args in zip(killpg, killpg_expected_calls):
        assert killpg_call[0] == exp_args, "Expected SIGTERM, *then* SIGKILL"


def test_restarts_running_endpoint_with_cached_args(epmanager, mocker):
    *_, em = epmanager
    child_pid = random.randrange(1, 32768 + 1)
    args_tup = (
        pwd.getpwnam(getpass.getuser()),
        [],
        {"name": "some_ep_name", "user_opts": {"heartbeat": 10}},
    )

    mock_os = mocker.patch(f"{_MOCK_BASE}os")
    mock_os.waitpid.side_effect = [(child_pid, -1), (0, -1)]
    mock_os.waitstatus_to_exitcode.return_value = 0

    em._cached_cmd_start_args[child_pid] = args_tup
    em.cmd_start_endpoint = mocker.Mock()

    em.wait_for_children()

    assert em.cmd_start_endpoint.call_args.args == args_tup


def test_no_cached_args_means_no_restart(epmanager, mocker):
    *_, em = epmanager
    child_pid = random.randrange(1, 32768 + 1)

    mock_os = mocker.patch(f"{_MOCK_BASE}os")
    mock_os.waitpid.side_effect = [(child_pid, -1), (0, -1)]
    mock_os.waitstatus_to_exitcode.return_value = -127
    em.cmd_start_endpoint = mocker.Mock()

    em.wait_for_children()

    assert em.cmd_start_endpoint.call_count == 0


def test_emits_endpoint_id_if_isatty(mocker, epmanager):
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    *_, em = epmanager

    mocker.patch.object(em, "_install_signal_handlers", side_effect=Exception)

    mock_sys = mocker.patch(f"{_MOCK_BASE}sys")
    mock_sys.stdout.isatty.return_value = True
    mock_sys.stderr.isatty.return_value = True
    with pytest.raises(Exception):
        em.start()

    assert mock_log.info.called, "Always emitted to log"
    a = mock_log.info.call_args[0][0]
    assert em._endpoint_uuid_str in a
    assert mock_sys.stdout.write.called
    assert not mock_sys.stderr.write.called, "Expect ID not emitted twice"
    written = "".join(a[0] for a, _ in mock_sys.stdout.write.call_args_list)
    assert em._endpoint_uuid_str in written

    mock_log.reset_mock()
    mock_sys.reset_mock()
    mock_sys.stdout.isatty.return_value = False
    mock_sys.stderr.isatty.return_value = True
    with pytest.raises(Exception):
        em.start()

    assert mock_log.info.called, "Always emitted to log"
    a = mock_log.info.call_args[0][0]
    assert em._endpoint_uuid_str in a
    assert not mock_sys.stdout.write.called, "Expect ID not emitted twice"
    assert mock_sys.stderr.write.called
    written = "".join(a[0] for a, _ in mock_sys.stderr.write.call_args_list)
    assert em._endpoint_uuid_str in written

    mock_log.reset_mock()
    mock_sys.reset_mock()
    mock_sys.stdout.isatty.return_value = False
    mock_sys.stderr.isatty.return_value = False
    with pytest.raises(Exception):
        em.start()

    assert mock_log.info.called, "Always emitted to log"
    a = mock_log.info.call_args[0][0]
    assert em._endpoint_uuid_str in a
    assert not mock_sys.stdout.write.called
    assert not mock_sys.stderr.write.called


def test_warns_of_no_local_lookup(mocker, epmanager):
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    conf_dir, mock_conf, mock_client, em = epmanager

    em._time_to_stop = True
    em._event_loop()

    assert mock_log.error.called
    a = mock_log.error.call_args[0][0]
    assert "FileNotFoundError" in a, "Expected class name in error output -- help dev!"
    assert " unable to respond " in a
    assert " restart " in a
    assert f"{os.getpid()}" in a


def test_iterates_even_if_no_commands(mocker, epmanager):
    conf_dir, mock_conf, mock_client, em = epmanager
    mocker.patch(f"{_MOCK_BASE}log")  # silence logs

    em._command_stop_event.set()
    em._event_loop()  # subtest is that it iterates and doesn't block

    em._time_to_stop = False
    em._command_queue = mocker.Mock()
    em._command_queue.get.side_effect = queue.Empty()
    em._event_loop()

    assert em._command_queue.get.called


def test_emits_command_requested_debug(mocker, epmanager):
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    conf_dir, mock_conf, mock_client, em = epmanager
    em._command_queue = mocker.Mock()
    em._command_stop_event.set()

    props = pika.BasicProperties(
        content_type="application/json",
        content_encoding="utf-8",
        timestamp=round(time.time()),
        expiration="10000",
    )

    queue_item = [1, props, json.dumps({"asdf": 123}).encode()]
    em._command_queue.get.side_effect = [queue_item, queue.Empty()]
    em._event_loop()
    assert not mock_log.warning.called

    props.headers = {"debug": False}
    em._command_queue.get.side_effect = [queue_item, queue.Empty()]
    em._time_to_stop = False
    em._event_loop()
    assert not mock_log.warning.called

    props.headers = {"debug": True}
    em._command_queue.get.side_effect = [queue_item, queue.Empty()]
    em._time_to_stop = False
    em._event_loop()
    assert mock_log.warning.called
    a = mock_log.warning.call_args[0][0]

    assert "Command debug requested" in a
    assert f"Delivery Tag: {queue_item[0]}" in a
    assert f"Properties: {queue_item[1]}" in a
    assert f"Body bytes: {queue_item[2]}" in a
    assert em._command.ack.called, "Command always ACKed"


def test_emitted_debug_command_credentials_removed(mocker, epmanager, randomstring):
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    conf_dir, mock_conf, mock_client, em = epmanager
    em._command_queue = mocker.Mock()
    em._command_stop_event.set()

    props = pika.BasicProperties(
        content_type="application/json",
        content_encoding="utf-8",
        timestamp=round(time.time()),
        expiration="10000",
        headers={"debug": True},
    )

    pword = randomstring()
    pld = {"creds": f"scheme://user:{pword}@some.fqdn:1234/some/path"}
    queue_item = [1, props, json.dumps(pld).encode()]
    em._command_queue.get.side_effect = [queue_item, queue.Empty()]
    em._event_loop()
    assert mock_log.warning.called

    expected = _redact_url_creds(pld["creds"], redact_user=False)
    a = mock_log.warning.call_args[0][0]
    assert "Body bytes:" in a, "Verify test setup"
    assert pld["creds"] not in a
    assert pword not in a
    assert expected in a
    assert em._command.ack.called, "Command always ACKed"


def test_command_verifies_content_type(mocker, epmanager):
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    conf_dir, mock_conf, mock_client, em = epmanager
    em._command_queue = mocker.Mock()
    em._command_stop_event.set()

    props = pika.BasicProperties(
        content_type="asdfasdfasdfasd",  # the test
        content_encoding="utf-8",
        timestamp=round(time.time()),
        expiration="10000",
    )

    queue_item = [1, props, json.dumps({"asdf": 123}).encode()]
    em._command_queue.get.side_effect = [queue_item, queue.Empty()]
    em._event_loop()
    assert mock_log.error.called
    a = mock_log.error.call_args[0][0]
    assert "Unable to deserialize Globus Compute services command" in a
    assert "Invalid message type; expecting JSON" in a
    assert em._command.ack.called, "Command always ACKed"


def test_ignores_stale_commands(mocker, epmanager):
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    conf_dir, mock_conf, mock_client, em = epmanager
    em._command_queue = mocker.Mock()
    em._command_stop_event.set()

    with open("local_user_lookup.json", "w") as f:
        json.dump({"a": "a_user"}, f)

    props = pika.BasicProperties(
        content_type="application/json",  # the test
        content_encoding="utf-8",
        timestamp=round(time.time()) + 10 * 60,  # ten-minute clock skew, "apparently"
        expiration="10000",
    )

    queue_item = [1, props, json.dumps({"asdf": 123}).encode()]
    em._command_queue.get.side_effect = [queue_item, queue.Empty()]
    em._event_loop()
    assert mock_log.warning.called
    a = mock_log.warning.call_args[0][0]
    assert "Ignoring command from server" in a
    assert "Command too old or skew between" in a
    assert "Command timestamp: " in a
    assert "Endpoint timestamp: " in a
    assert em._command.ack.called, "Command always ACKed"


def test_handles_invalid_server_msg_gracefully(mocker, epmanager):
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    conf_dir, mock_conf, mock_client, em = epmanager

    props = pika.BasicProperties(
        content_type="application/json",
        content_encoding="utf-8",
        timestamp=round(time.time()),
        expiration="10000",
    )

    queue_item = (1, props, json.dumps({"asdf": 123}).encode())

    em._command_queue = mocker.Mock()
    em._command_stop_event.set()
    em._command_queue.get.side_effect = [queue_item, queue.Empty()]
    em._event_loop()
    a = mock_log.error.call_args[0][0]
    assert "Invalid server command" in a
    assert "KeyError" in a, "Expected exception name in log line"
    assert em._command.ack.called, "Command always ACKed"


def test_handles_unknown_user_gracefully(mocker, epmanager):
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    conf_dir, mock_conf, mock_client, em = epmanager

    props = pika.BasicProperties(
        content_type="application/json",
        content_encoding="utf-8",
        timestamp=round(time.time()),
        expiration="10000",
    )

    pld = {"globus_uuid": 1, "globus_username": 1}
    queue_item = (1, props, json.dumps(pld).encode())

    em._command_queue = mocker.Mock()
    em._command_stop_event.set()
    em._command_queue.get.side_effect = [queue_item, queue.Empty()]
    em._event_loop()
    a = mock_log.warning.call_args[0][0]
    assert "Invalid or unknown user" in a
    assert "KeyError" in a, "Expected exception name in log line"
    assert em._command.ack.called, "Command always ACKed"


def test_handles_unknown_local_username_gracefully(mocker, epmanager):
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    conf_dir, mock_conf, mock_client, em = epmanager

    with open("local_user_lookup.json", "w") as f:
        json.dump({"a": "a_user"}, f)

    props = pika.BasicProperties(
        content_type="application/json",
        content_encoding="utf-8",
        timestamp=round(time.time()),
        expiration="10000",
    )

    pld = {
        "globus_uuid": "a",
        "globus_username": "a",
    }
    queue_item = (1, props, json.dumps(pld).encode())

    mocker.patch(f"{_MOCK_BASE}pwd.getpwnam", side_effect=Exception())

    em._command_queue = mocker.Mock()
    em._command_stop_event.set()
    em._command_queue.get.side_effect = [queue_item, queue.Empty()]
    em._event_loop()
    a = mock_log.warning.call_args[0][0]
    assert "Invalid or unknown local user" in a
    assert em._command.ack.called, "Command always ACKed"


@pytest.mark.parametrize(
    "cmd_name", ("", "_private", "9c", "valid_but_do_not_exist", " ", "a" * 101)
)
def test_handles_invalid_command_gracefully(mocker, epmanager, cmd_name):
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    conf_dir, mock_conf, mock_client, em = epmanager

    with open("local_user_lookup.json", "w") as f:
        json.dump({"a": "a_user"}, f)

    props = pika.BasicProperties(
        content_type="application/json",
        content_encoding="utf-8",
        timestamp=round(time.time()),
        expiration="10000",
    )

    pld = {
        "globus_uuid": "a",
        "globus_username": "a",
        "command": cmd_name,
        "user_opts": {"heartbeat": 10},
    }
    queue_item = (1, props, json.dumps(pld).encode())

    mocker.patch(f"{_MOCK_BASE}pwd.getpwnam")

    em._command_queue = mocker.Mock()
    em._command_stop_event.set()
    em._command_queue.get.side_effect = [queue_item, queue.Empty()]
    em._event_loop()
    a = mock_log.error.call_args[0][0]
    assert "Unknown or invalid command" in a
    assert str(cmd_name) in a
    assert em._command.ack.called, "Command always ACKed"


def test_handles_failed_command(mocker, epmanager):
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    mocker.patch(
        f"{_MOCK_BASE}EndpointManager.cmd_start_endpoint", side_effect=Exception()
    )
    conf_dir, mock_conf, mock_client, em = epmanager

    with open("local_user_lookup.json", "w") as f:
        json.dump({"a": "a_user"}, f)

    props = pika.BasicProperties(
        content_type="application/json",
        content_encoding="utf-8",
        timestamp=round(time.time()),
        expiration="10000",
    )

    pld = {
        "globus_uuid": "a",
        "globus_username": "a",
        "command": "cmd_start_endpoint",
        "user_opts": {"heartbeat": 10},
    }
    queue_item = (1, props, json.dumps(pld).encode())

    mocker.patch(f"{_MOCK_BASE}pwd.getpwnam")

    em._command_queue = mocker.Mock()
    em._command_stop_event.set()
    em._command_queue.get.side_effect = [queue_item, queue.Empty()]
    em._event_loop()
    a = mock_log.exception.call_args[0][0]
    assert "Unable to execute command" in a
    assert pld["command"] in a, "Expected debugging help in log"
    assert "   args: " in a, "Expected debugging help in log"
    assert " kwargs: " in a, "Expected debugging help in log"
    assert em._command.ack.called, "Command always ACKed"


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT, signal.SIGQUIT])
def test_handles_shutdown_signal(successful_exec, sig, reset_signals):
    mock_os, _conf_dir, _mock_conf, _mock_client, em = successful_exec

    with mock.patch.object(em, "_install_signal_handlers") as mock_install:
        mock_install.side_effect = Exception()
        with pytest.raises(Exception):
            em.start()
        assert mock_install.called, "Ensure hookup that installs signal handlers ..."

    em._install_signal_handlers()  # ... now install them for real ...
    assert em._time_to_stop is False
    os.kill(os.getpid(), sig)
    em._event_loop()

    assert em._time_to_stop is True
    assert not em._command_queue.get.called, " ... that we've now confirmed works"


def test_environment_default_path(successful_exec, mocker):
    mock_os, *_, em = successful_exec
    with pytest.raises(SystemExit) as pyexc:
        em._event_loop()

    expected_pybindir = pathlib.Path(sys.executable).parent
    expected_order = ("/usr/local/bin", "/usr/bin", "/bin", str(expected_pybindir))

    assert pyexc.value.code == 87, "Q&D: verify we exec'ed, based on '+= 1'"
    a, k = mock_os.execvpe.call_args
    env = k["env"]
    assert "PATH" in env, "Path always set, with default if nothing else available"
    for expected_dir, found_dir in zip(expected_order, env["PATH"].split(":")):
        assert expected_dir == found_dir, "Expected sane default path order"


def test_loads_user_environment(successful_exec, randomstring):
    mock_os, conf_dir, _mock_conf, _mock_client, em = successful_exec

    sentinel_key = randomstring()
    expected_env = {sentinel_key: randomstring()}
    (conf_dir / "user_environment.yaml").write_text(yaml.dump(expected_env))
    with pytest.raises(SystemExit) as pyexc:
        em._event_loop()

    assert pyexc.value.code == 87, "Q&D: verify we exec'ed, based on '+= 1'"
    a, k = mock_os.execvpe.call_args
    env = k["env"]
    assert sentinel_key in env
    assert env[sentinel_key] == expected_env[sentinel_key]


def test_handles_invalid_user_environment_file_gracefully(successful_exec, mocker):
    mock_os, conf_dir, _mock_conf, _mock_client, em = successful_exec
    mock_warn = mocker.patch(f"{_MOCK_BASE}log.warning")

    env_path = conf_dir / "user_environment.yaml"
    env_path.write_text("\nalkdhj: g\nkladhj - asdf -asd f")
    with pytest.raises(SystemExit) as pyexc:
        em._event_loop()
    assert pyexc.value.code == 87, "Q&D: verify we exec'ed, based on '+= 1'"
    a, k = mock_warn.call_args_list[0]
    assert "Failed to parse user environment variables" in a[0]
    assert env_path in a, "Expected pointer to problem file in warning"
    assert "ScannerError" in a, "Expected exception name in warning"


def test_environment_default_path_set_if_not_specified(successful_exec):
    mock_os, conf_dir, _mock_conf, _mock_client, em = successful_exec

    expected_env = {"some_env_var": "some value"}
    (conf_dir / "user_environment.yaml").write_text(yaml.dump(expected_env))
    with pytest.raises(SystemExit) as pyexc:
        em._event_loop()

    assert pyexc.value.code == 87, "Q&D: verify we exec'ed, based on '+= 1'"
    a, k = mock_os.execvpe.call_args
    env = k["env"]
    assert "PATH" in env, "Expected PATH is always set"


def test_warns_if_executable_not_found(successful_exec, mocker):
    mock_os, conf_dir, _mock_conf, _mock_client, em = successful_exec
    mock_warn = mocker.patch(f"{_MOCK_BASE}log.warning")

    expected_env = {"PATH": "/some/typoed:/path:/here"}
    (conf_dir / "user_environment.yaml").write_text(yaml.dump(expected_env))
    with pytest.raises(SystemExit):
        em._event_loop()

    assert mock_warn.called
    a, k = mock_warn.call_args
    assert "Unable to find executable" in a[0], "Expected precise problem in warning"
    assert "(not found):" in a[0]
    assert "globus-compute-endpoint" in a[0], "Share the precise thing not-found"
    assert expected_env["PATH"] in a[0]


def test_start_endpoint_children_die_with_parent(successful_exec):
    mock_os, _conf_dir, _mock_conf, _mock_client, em = successful_exec
    with pytest.raises(SystemExit) as pyexc:
        em._event_loop()

    assert pyexc.value.code == 87, "Q&D: verify we exec'ed, based on '+= 1'"
    a, k = mock_os.execvpe.call_args
    assert a[0] == "globus-compute-endpoint", "Sanity check"
    assert k["args"][0] == a[0], "Expect transparency for admin"
    assert k["args"][-1] == "--die-with-parent"  # trust flag to do the hard work


def test_start_endpoint_children_have_own_session(successful_exec):
    mock_os, _conf_dir, _mock_conf, _mock_client, em = successful_exec
    with pytest.raises(SystemExit) as pyexc:
        em._event_loop()

    assert pyexc.value.code == 87, "Q&D: verify we exec'ed, based on '+= 1'"
    assert mock_os.setsid.called


def test_start_endpoint_privileges_dropped(successful_exec):
    mock_os, _conf_dir, _mock_conf, _mock_client, em = successful_exec
    with pytest.raises(SystemExit) as pyexc:
        em._event_loop()

    assert pyexc.value.code == 87, "Q&D: verify we exec'ed, based on '+= 1'"

    expected_user = getpass.getuser()
    expected_gid = os.getgid()  # "cheating"; exploit knowledge of test setup
    expected_uid = os.getuid()  # "cheating"; exploit knowledge of test setup
    assert mock_os.initgroups.called
    (uname, gid), _ = mock_os.initgroups.call_args
    assert uname == expected_user
    assert gid == expected_gid

    assert mock_os.setresgid.called, "Do NOT save gid; truly change user"
    a, _ = mock_os.setresgid.call_args
    assert a == (expected_gid, expected_gid, expected_gid)

    assert mock_os.setresuid.called, "Do NOT save uid; truly change user"
    a, _ = mock_os.setresuid.call_args
    assert a == (expected_uid, expected_uid, expected_uid)


def test_run_as_same_user_disabled_if_admin(mocker, conf_dir, mock_conf, mock_client):
    ep_uuid, mock_gcc = mock_client

    mock_pwd = mocker.patch(f"{_MOCK_BASE}pwd")
    mock_prctl = mocker.patch(f"{_MOCK_BASE}pyprctl")
    mock_prctl.CapState.get_current.return_value.effective = set()

    mock_pwd.getpwuid.return_value = namedtuple("getent", "pw_name,pw_uid")("asdf", 0)
    em = EndpointManager(conf_dir, ep_uuid, mock_conf)
    assert em._allow_same_user is False, "Verify check against UID 0"

    mock_pwd.getpwuid.return_value = namedtuple("getent", "pw_name,pw_uid")("root", 999)
    em = EndpointManager(conf_dir, ep_uuid, mock_conf)
    assert em._allow_same_user is False, "Verify check against 'root' username"


@pytest.mark.parametrize("cap", (pyprctl.Cap.SYS_ADMIN, pyprctl.Cap.SETUID))
def test_run_as_same_user_disabled_if_privileged(
    mocker, conf_dir, mock_conf, mock_client, cap
):
    # spot-check a couple of capabilities: if set, then same user is *disallowed*
    ep_uuid, mock_gcc = mock_client

    _test_mock_base = "globus_compute_endpoint.endpoint.utils."
    mocker.patch(f"{_test_mock_base}_pwd")
    mock_prctl = mocker.patch(f"{_test_mock_base}_pyprctl")

    mock_prctl.CapState.get_current.return_value.effective = {cap}
    em = EndpointManager(conf_dir, ep_uuid, mock_conf)
    assert em._allow_same_user is False


def test_run_as_same_user_enabled_if_not_admin(
    mocker, conf_dir, mock_conf, mock_client
):
    # spot-check a couple of capabilities: if set, then same user is *disallowed*
    ep_uuid, mock_gcc = mock_client

    _test_mock_base = "globus_compute_endpoint.endpoint.utils."
    mocker.patch(f"{_test_mock_base}_pwd")
    mocker.patch(f"{_test_mock_base}_pyprctl")

    em = EndpointManager(conf_dir, ep_uuid, mock_conf)
    assert em._allow_same_user is True, "If not privileged, can only runas same user"


def test_run_as_same_user_forced_warns(
    mocker, conf_dir, mock_conf, mock_client, randomstring
):
    # spot-check a couple of capabilities: if set, then same user is *disallowed*
    ep_uuid, mock_gcc = mock_client

    mocker.patch(f"{_MOCK_BASE}pwd")
    mock_os = mocker.patch(f"{_MOCK_BASE}os")
    mock_warn = mocker.patch(f"{_MOCK_BASE}log.warning")

    _test_mock_base = "globus_compute_endpoint.endpoint.utils."
    mocker.patch(f"{_test_mock_base}_pwd")
    mock_prctl = mocker.patch(f"{_test_mock_base}_pyprctl")

    mock_prctl.CapState.get_current.return_value.effective = {pyprctl.Cap.SYS_ADMIN}
    em = EndpointManager(conf_dir, ep_uuid, mock_conf)
    assert em._allow_same_user is False, "Verify test setup"
    assert not mock_warn.called, "Verify test setup"

    mock_uid, mock_gid = randomstring(), randomstring()
    mock_os.getuid.return_value = mock_uid
    mock_os.getgid.return_value = mock_gid
    mock_conf.force_mu_allow_same_user = True
    em = EndpointManager(conf_dir, ep_uuid, mock_conf)
    assert em._allow_same_user is True
    assert mock_warn.called

    a, _k = mock_warn.call_args
    assert "`force_mu_allow_same_user` set to true" in a[0]
    assert "very dangerous override" in a[0]
    assert "Endpoint (UID, GID):" in a[0], "Expect process UID, GID in warning"
    assert f"({mock_uid}, {mock_gid})" in a[0], "Expect process UID, GID in warning"


def test_run_as_same_user_fails_if_admin(successful_exec):
    mock_os, _conf_dir, _mock_conf, _mock_client, em = successful_exec

    pw_rec = pwd.getpwuid(os.getuid())
    em._allow_same_user = False
    kwargs = {"name": "some_endpoint_name"}
    with pytest.raises(InvalidUserError) as pyexc:
        em.cmd_start_endpoint(pw_rec, None, kwargs)

    assert "UID is same as" in str(pyexc.value)
    assert "using a non-root user" in str(pyexc.value), "Expected suggested fix"
    assert "removing privileges" in str(pyexc.value), "Expected suggested fix"


def test_run_as_same_user_does_not_change_uid(successful_exec):
    mock_os, _conf_dir, _mock_conf, _mock_client, em = successful_exec
    mock_os.getuid.return_value = os.getuid()
    mock_os.getgid.return_value = os.getgid()

    em._allow_same_user = True
    with pytest.raises(SystemExit) as pyexc:
        em._event_loop()

    assert pyexc.value.code == 84, "Q&D: verify we exec'ed, but no privilege drop"

    assert not mock_os.initgroups.called
    assert not mock_os.setresuid.called
    assert not mock_os.setresgid.called


def test_default_to_secure_umask(successful_exec):
    mock_os, _conf_dir, _mock_conf, _mock_client, em = successful_exec
    with pytest.raises(SystemExit) as pyexc:
        em._event_loop()

    assert pyexc.value.code == 87, "Q&D: verify we exec'ed, based on '+= 1'"

    assert mock_os.umask.called
    umask = mock_os.umask.call_args[0][0]
    assert umask == 0o77


def test_start_from_user_dir(successful_exec):
    mock_os, _conf_dir, _mock_conf, _mock_client, em = successful_exec
    with pytest.raises(SystemExit) as pyexc:
        em._event_loop()

    assert pyexc.value.code == 87, "Q&D: verify we exec'ed, based on '+= 1'"

    ud = mock_os.chdir.call_args[0][0]
    assert ud == str(pathlib.Path.home())  # "cheating"; exploit knowledge of test setup


def test_all_files_closed(successful_exec):
    mock_os, _conf_dir, _mock_conf, _mock_client, em = successful_exec
    with pytest.raises(SystemExit) as pyexc:
        em._event_loop()

    assert pyexc.value.code == 87, "Q&D: verify we exec'ed, based on '+= 1'"

    _soft_no, hard_no = resource.getrlimit(resource.RLIMIT_NOFILE)
    assert mock_os.closerange.called
    (low, hi), _ = mock_os.closerange.call_args
    assert low == 3, "Starts from first FD number after std* files"
    assert low < hi
    assert hi >= hard_no, "Expect ALL open files closed"

    assert mock_os.dup2.call_count == 3, "Expect to close 3 std* files"
    closed = [std_to_close for (_fd, std_to_close), _ in mock_os.dup2.call_args_list]
    assert closed == [0, 1, 2]


@pytest.mark.parametrize("conf_size", [10, 222, 223, 300])
def test_pipe_size_limit(mocker, successful_exec, conf_size):
    _mock_os, _conf_dir, _mock_conf, _mock_client, em = successful_exec
    mock_log = mocker.patch(f"{_MOCK_BASE}log")

    conf_str = "$" * conf_size

    # Add 34 bytes for dict keys, etc.
    stdin_data_size = conf_size + 34
    pipe_buffer_size = 512
    # Subtract 256 for hard-coded buffer in-code
    is_valid = pipe_buffer_size - 256 - stdin_data_size >= 0

    mocker.patch.object(fcntl, "fcntl", return_value=pipe_buffer_size)
    mocker.patch(f"{_MOCK_BASE}render_config_user_template", return_value=conf_str)

    with pytest.raises(SystemExit) as pyexc:
        em._event_loop()

    if is_valid:
        assert pyexc.value.code == 87, "Q&D: verify we exec'ed, based on '+= 1'"
    else:
        assert pyexc.value.code < 87
        assert f"{stdin_data_size} bytes" in mock_log.error.call_args[0][0]


@pytest.mark.parametrize(
    "data",
    [
        (True, {"heartbeat": 10}),
        (True, {"heartbeat": 10, "foo": "bar"}),
        (False, {}),
        (False, {"foo": "bar"}),
    ],
)
def test_render_user_config(conf_dir: pathlib.Path, data):
    is_valid, user_opts = data

    template = Endpoint.user_config_template_path(conf_dir)
    template.write_text("heartbeat_period: {{ heartbeat }}")

    if is_valid:
        rendered = render_config_user_template(conf_dir, user_opts)
        rendered_dict = yaml.safe_load(rendered)
        assert rendered_dict["heartbeat_period"] == user_opts["heartbeat"]
    else:
        with pytest.raises(jinja2.exceptions.UndefinedError) as e:
            render_config_user_template(conf_dir, user_opts)
            assert "Missing required" in str(e)


def test_render_user_config_escape_strings(conf_dir: pathlib.Path):
    template = Endpoint.user_config_template_path(conf_dir)
    template.write_text(
        """
endpoint_setup: {{ setup }}
engine:
    type: {{ engine.type }}
    accelerators:
        {%- for a in engine.accelerators %}
        - {{ a }}
        {% endfor %}
        """
    )

    user_opts = {
        "setup": f"my-setup\nallowed_functions:\n    - {uuid.uuid4()}",
        "engine": {
            "type": "GlobusComputeEngine\n    task_status_queue: bad_boy_queue",
            "accelerators": [f"{uuid.uuid4()}\n    mem_per_worker: 100"],
        },
    }
    rendered = render_config_user_template(conf_dir, user_opts)
    rendered_dict = yaml.safe_load(rendered)

    assert len(rendered_dict) == 2
    assert len(rendered_dict["engine"]) == 2
    assert rendered_dict["endpoint_setup"] == user_opts["setup"]
    assert rendered_dict["engine"]["type"] == user_opts["engine"]["type"]
    assert (
        rendered_dict["engine"]["accelerators"] == user_opts["engine"]["accelerators"]
    )


@pytest.mark.parametrize(
    "data",
    [
        (True, 10),
        (True, "bar"),
        (True, 10.0),
        (True, ["bar", 10]),
        (True, {"bar": 10}),
        (False, ("bar", 10)),
        (False, {"bar", 10}),
        (False, str),
        (False, Exception),
        (False, locals),
    ],
)
def test_render_user_config_option_types(conf_dir: pathlib.Path, data):
    is_valid, val = data

    template = Endpoint.user_config_template_path(conf_dir)
    template.write_text("foo: {{ foo }}")

    user_opts = {"foo": val}

    if is_valid:
        render_config_user_template(conf_dir, user_opts)
    else:
        with pytest.raises(ValueError) as pyt_exc:
            render_config_user_template(conf_dir, user_opts)
        assert "not a valid user config option type" in pyt_exc.exconly()


@pytest.mark.parametrize(
    "data",
    [
        ("{{ foo.__class__ }}", "bar"),
        ("{{ foo.__code__ }}", lambda: None),
        ("{{ foo._priv }}", type("Foo", (object,), {"_priv": "secret"})()),
    ],
)
def test_render_user_config_sandbox(
    mocker: MockFixture, conf_dir: pathlib.Path, data: t.Tuple[str, t.Any]
):
    jinja_op, val = data

    template = Endpoint.user_config_template_path(conf_dir)
    template.write_text(f"foo: {jinja_op}")

    user_opts = {"foo": val}
    mocker.patch(
        "globus_compute_endpoint.endpoint.config.utils._sanitize_user_opts",
        return_value=user_opts,
    )

    with pytest.raises(jinja2.exceptions.SecurityError):
        render_config_user_template(conf_dir, user_opts)


@pytest.mark.parametrize(
    "data",
    [
        (True, "-lh"),
        (True, "'-lh'"),
        (True, "'-l' '-h'"),
        (True, "-lh; rm -rf"),
        (True, '-lh && "rm -rf"'),
        (True, '"-lh && "rm -rf"'),
        (True, '-lh && rm -rf"'),
        (True, "\0Do this thing"),
        (True, '\r"-lh && rm -rf"'),
        (True, '\n"-lh && rm -rf"'),
        (True, '"-lh && \\u0015rm -rf"'),
        (True, "-lh\nbad: boy"),
        (False, 10),
        (False, "10"),
    ],
)
def test_render_user_config_shell_escape(
    conf_dir: pathlib.Path, data: t.Tuple[bool, t.Any]
):
    is_valid, option = data

    template = Endpoint.user_config_template_path(conf_dir)
    template.write_text("option: {{ option|shell_escape }}")

    user_opts = {"option": option}
    rendered = render_config_user_template(conf_dir, user_opts)
    rendered_dict = yaml.safe_load(rendered)

    assert len(rendered_dict) == 1
    rendered_option = rendered_dict["option"]
    if is_valid:
        escaped_option = shlex.quote(option)
        assert f"ls {rendered_option}" == f"ls {escaped_option}"
    else:
        assert rendered_option == option


@pytest.mark.parametrize("schema_exists", [True, False])
def test_render_user_config_apply_schema(
    mocker: MockFixture, conf_dir: pathlib.Path, schema_exists: bool
):
    template = Endpoint.user_config_template_path(conf_dir)
    template.write_text("foo: {{ foo }}")
    if schema_exists:
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "foo": {"type": "string"},
            },
        }
        schema_path = Endpoint.user_config_schema_path(conf_dir)
        schema_path.write_text(json.dumps(schema))

    mock_validate = mocker.patch.object(jsonschema, "validate")

    user_opts = {"foo": "bar"}
    render_config_user_template(conf_dir, user_opts)

    if schema_exists:
        assert mock_validate.called
        *_, kwargs = mock_validate.call_args
        assert kwargs["instance"] == user_opts
        assert kwargs["schema"] == schema
    else:
        assert not mock_validate.called


@pytest.mark.parametrize(
    "data",
    [
        (True, {"foo": "bar", "nest": {"nested": 10}}),
        (True, {"foo": "bar", "extra": "ok"}),
        (False, {"foo": 10}),
        (False, {"foo": {"nested": "bar"}}),
        (False, {"nest": "nested"}),
        (False, {"nest": {"nested": "blah", "extra": "baddie"}}),
    ],
)
def test_validate_user_config_options(mocker: MockFixture, data: t.Tuple[bool, dict]):
    is_valid, user_opts = data

    mock_log = mocker.patch("globus_compute_endpoint.endpoint.config.utils.log")

    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "foo": {"type": "string"},
            "nest": {
                "type": "object",
                "properties": {"nested": {"type": "number"}},
                "additionalProperties": False,
            },
        },
    }
    if is_valid:
        _validate_user_opts(user_opts, schema)
    else:
        with pytest.raises(jsonschema.ValidationError):
            _validate_user_opts(user_opts, schema)
        assert mock_log.error.called
        a, *_ = mock_log.error.call_args
        assert "user config options are invalid" in str(a)


@pytest.mark.parametrize("schema", ["foo", {"type": "blah"}])
def test_validate_user_config_options_invalid_schema(
    mocker: MockFixture, schema: t.Any
):
    mock_log = mocker.patch("globus_compute_endpoint.endpoint.config.utils.log")
    user_opts = {"foo": "bar"}
    with pytest.raises(jsonschema.SchemaError):
        _validate_user_opts(user_opts, schema)
    assert mock_log.error.called
    a, *_ = mock_log.error.call_args
    assert "user config schema is invalid" in str(a)


@pytest.mark.parametrize(
    "data", [(True, '{"foo": "bar"}'), (False, '{"foo": "bar", }')]
)
def test_load_user_config_schema(
    mocker: MockFixture, conf_dir: pathlib.Path, data: t.Tuple[bool, str]
):
    is_valid, schema_json = data

    mock_log = mocker.patch("globus_compute_endpoint.endpoint.config.utils.log")

    template = Endpoint.user_config_schema_path(conf_dir)
    template.write_text(schema_json)

    if is_valid:
        schema = load_user_config_schema(conf_dir)
        assert schema == json.loads(schema_json)
    else:
        with pytest.raises(json.JSONDecodeError):
            load_user_config_schema(conf_dir)
        assert mock_log.error.called
        a, *_ = mock_log.error.call_args
        assert "user config schema is not valid JSON" in str(a)


@pytest.mark.parametrize("port", [random.randint(0, 65535)])
def test_port_is_respected(mocker, mock_client, mock_conf, conf_dir, port):
    ep_uuid, _ = mock_client
    mock_conf.amqp_port = port

    mock_update_url_port = mocker.patch(f"{_MOCK_BASE}update_url_port")

    EndpointManager(conf_dir, ep_uuid, mock_conf)

    assert mock_update_url_port.call_args[0][1] == port
