import getpass
import json
import os
import pathlib
import queue
import random
import re
import resource
import signal
import time
import uuid
from unittest import mock

import pika
import pytest as pytest
import responses
from globus_compute_endpoint.endpoint.endpoint_manager import EndpointManager
from globus_compute_endpoint.endpoint.utils import _redact_url_creds
from globus_compute_endpoint.endpoint.utils.config import Config
from globus_sdk import GlobusAPIError, NetworkError

_MOCK_BASE = "globus_compute_endpoint.endpoint.endpoint_manager."


@pytest.fixture
def conf_dir(fs):
    conf_dir = pathlib.Path("/some/path/mock_endpoint")
    conf_dir.mkdir(parents=True, exist_ok=True)
    yield conf_dir


@pytest.fixture
def mock_conf():
    yield Config(executors=[])


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
    em = EndpointManager(conf_dir, ep_uuid, mock_conf)
    em._command = mocker.Mock()

    yield conf_dir, mock_conf, mock_client, em


@pytest.fixture
def register_endpoint_failure_response():
    def create_response(status_code: int = 200, err_msg: str = "some error msg"):
        responses.add(
            method=responses.POST,
            url="https://api2.funcx.org/v2/endpoints",
            headers={"Content-Type": "application/json"},
            json={"error": err_msg},
            status=status_code,
        )

    return create_response


@pytest.fixture
def successful_exec(mocker, epmanager):
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
        "command": "start_endpoint",
        "kwargs": {"name": "some_ep_name"},
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
    assert "*(" in a[0], "Expected asterisk as subtle clue of 'multi-tenant'"
    assert f"{ep_uuid}, {conf_dir.name}" in a[0], "Can find process by conf"

    if env:
        assert f" - {env}" in a[0], "Expected environment name in title"
    else:
        assert " - " not in a[0], "Default is not 'do not show env' for prod"
    assert a[0].endswith(f"[{orig_proc_title}]"), "Save original cmdline for debugging"


@responses.activate
@pytest.mark.parametrize("status_code", [409, 423, 418])
def test_gracefully_exits_if_in_conflict_or_locked(
    mocker,
    register_endpoint_failure_response,
    conf_dir,
    mock_conf,
    endpoint_uuid,
    randomstring,
    get_standard_compute_client,
    status_code,
):
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    mock_gcc = get_standard_compute_client()
    mocker.patch("globus_compute_sdk.Client", return_value=mock_gcc)

    some_err = randomstring()
    register_endpoint_failure_response(status_code, some_err)

    with pytest.raises((GlobusAPIError, SystemExit)) as pyexc:
        EndpointManager(conf_dir, endpoint_uuid, mock_conf)

    if status_code in (409, 423):
        assert pyexc.value.code == os.EX_UNAVAILABLE, "Expecting meaningful exit code"
        assert mock_log.warning.called
        a, *_ = mock_log.warning.call_args
        assert some_err in str(a), "Expected upstream response still shared"
    else:
        # The other route tests SystemExit; nominally this route is an unhandled
        # traceback -- good.  We should _not_ blanket hide all exceptions.
        assert pyexc.value.http_status == status_code


def test_sends_metadata_during_registration(conf_dir, mock_conf, mock_client):
    ep_uuid, mock_gcc = mock_client
    EndpointManager(conf_dir, ep_uuid, mock_conf)

    assert mock_gcc.register_endpoint.called
    _a, k = mock_gcc.register_endpoint.call_args
    for key in ("endpoint_version", "hostname", "local_user", "config"):
        assert key in k["metadata"], "Expected minimal metadata"

    for key in (
        "_type",
        "multi_tenant",
        "stdout",
        "stderr",
        "environment",
        "funcx_service_address",
    ):
        assert key in k["metadata"]["config"]

    assert k["metadata"]["config"]["multi_tenant"] is True


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


def test_writes_endpoint_uuid(epmanager):
    conf_dir, _mock_conf, mock_client, _em = epmanager
    _ep_uuid, mock_gcc = mock_client

    returned_uuid = mock_gcc.register_endpoint.return_value["endpoint_id"]

    ep_json_path = conf_dir / "endpoint.json"
    assert ep_json_path.exists()

    ep_data = json.loads(ep_json_path.read_text())
    assert ep_data["endpoint_id"] == returned_uuid


def test_log_contains_sentinel_lines(mocker, epmanager, noop):
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


def test_title_changes_for_shutdown(mocker, epmanager, noop, mock_setproctitle):
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


def test_children_signaled_at_shutdown(mocker, epmanager, randomstring, noop):
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
        em._child_args[pid] = expected[-1]

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
    killpg_expected_calls = [(pid, signal.SIGTERM) for pid in em._child_args]
    killpg_expected_calls.extend((pid, signal.SIGKILL) for pid in em._child_args)

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

    pld = {"globus_uuid": "a", "globus_username": "a", "command": cmd_name}
    queue_item = (1, props, json.dumps(pld).encode())

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
    mocker.patch(f"{_MOCK_BASE}EndpointManager.start_endpoint", side_effect=Exception())
    conf_dir, mock_conf, mock_client, em = epmanager

    with open("local_user_lookup.json", "w") as f:
        json.dump({"a": "a_user"}, f)

    props = pika.BasicProperties(
        content_type="application/json",
        content_encoding="utf-8",
        timestamp=round(time.time()),
        expiration="10000",
    )

    pld = {"globus_uuid": "a", "globus_username": "a", "command": "start_endpoint"}
    queue_item = (1, props, json.dumps(pld).encode())

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
def test_handles_shutdown_signal(successful_exec, sig):
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


def test_start_endpoint_children_die_with_parent(successful_exec):
    mock_os, _conf_dir, _mock_conf, _mock_client, em = successful_exec
    with pytest.raises(SystemExit) as pyexc:
        em._event_loop()

    assert pyexc.value.code == 85, "Q&D: verify we exec'ed, based on '+= 1'"
    a, k = mock_os.execvpe.call_args
    assert a[0] == "globus-compute-endpoint", "Sanity check"
    assert k["args"][0] == a[0], "Expect transparency for admin"
    assert k["args"][-1] == "--die-with-parent"  # trust flag to do the hard work


def test_start_endpoint_children_have_own_session(successful_exec):
    mock_os, _conf_dir, _mock_conf, _mock_client, em = successful_exec
    with pytest.raises(SystemExit) as pyexc:
        em._event_loop()

    assert pyexc.value.code == 85, "Q&D: verify we exec'ed, based on '+= 1'"
    assert mock_os.setsid.called


def test_start_endpoint_privileges_dropped(successful_exec):
    mock_os, _conf_dir, _mock_conf, _mock_client, em = successful_exec
    with pytest.raises(SystemExit) as pyexc:
        em._event_loop()

    assert pyexc.value.code == 85, "Q&D: verify we exec'ed, based on '+= 1'"

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


def test_default_to_secure_umask(successful_exec):
    mock_os, _conf_dir, _mock_conf, _mock_client, em = successful_exec
    with pytest.raises(SystemExit) as pyexc:
        em._event_loop()

    assert pyexc.value.code == 85, "Q&D: verify we exec'ed, based on '+= 1'"

    assert mock_os.umask.called
    umask = mock_os.umask.call_args[0][0]
    assert umask == 0o77


def test_start_from_user_dir(successful_exec):
    mock_os, _conf_dir, _mock_conf, _mock_client, em = successful_exec
    with pytest.raises(SystemExit) as pyexc:
        em._event_loop()

    assert pyexc.value.code == 85, "Q&D: verify we exec'ed, based on '+= 1'"

    ud = mock_os.chdir.call_args[0][0]
    assert ud == str(pathlib.Path.home())  # "cheating"; exploit knowledge of test setup


def test_all_files_closed(successful_exec):
    mock_os, _conf_dir, _mock_conf, _mock_client, em = successful_exec
    with pytest.raises(SystemExit) as pyexc:
        em._event_loop()

    assert pyexc.value.code == 85, "Q&D: verify we exec'ed, based on '+= 1'"

    _soft_no, hard_no = resource.getrlimit(resource.RLIMIT_NOFILE)
    assert mock_os.closerange.called
    (low, hi), _ = mock_os.closerange.call_args
    assert low == 3, "Starts from first FD number after std* files"
    assert low < hi
    assert hi >= hard_no, "Expect ALL open files closed"

    assert mock_os.dup2.call_count == 3, "Expect to close 3 std* files"
    closed = [std_to_close for (_fd, std_to_close), _ in mock_os.dup2.call_args_list]
    assert closed == [0, 1, 2]
