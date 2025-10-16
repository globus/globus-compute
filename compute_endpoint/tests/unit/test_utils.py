import resource
import sys
import uuid
from collections import namedtuple
from unittest import mock

import pika
import pytest
from globus_compute_endpoint.endpoint.utils import (
    _redact_url_creds,
    close_all_fds,
    is_privileged,
    send_endpoint_startup_failure_to_amqp,
    update_url_port,
)

try:
    import pyprctl  # noqa

    _has_pyprctl = True
except AttributeError:
    _has_pyprctl = False


_MOCK_BASE = "globus_compute_endpoint.endpoint.utils."


@pytest.fixture
def mock_mq_chan(mocker):
    _mq_conn = mocker.MagicMock(spec=pika.BlockingConnection)
    _mq_conn.__enter__.return_value = _mq_conn
    _mq_chan = mocker.MagicMock(spec=pika.adapters.blocking_connection.BlockingChannel)
    _mq_chan.__enter__.return_value = _mq_chan

    mock_pika = mocker.Mock(spec=pika)
    mock_pika.BlockingConnection.return_value = _mq_conn
    _mq_conn.channel.return_value = _mq_chan
    with mock.patch.dict(sys.modules, {"pika": mock_pika}):
        yield _mq_chan


def test_url_redaction(randomstring):
    scheme = randomstring()
    uname = randomstring()
    pword = randomstring()
    fqdn = randomstring()
    somepath = randomstring()
    some_url = f"{scheme}://{uname}:{pword}@{fqdn}/{somepath}"
    for redact_user in (True, False):
        kwargs = {"redact_user": redact_user}
        for repl in (None, "XxX", "*", "---"):
            if repl:
                kwargs["repl"] = repl
            else:
                repl = "***"  # default replacement

            if redact_user:
                expected = f"{scheme}://{repl}:{repl}@{fqdn}/{somepath}"
            else:
                expected = f"{scheme}://{uname}:{repl}@{fqdn}/{somepath}"
            assert _redact_url_creds(some_url, **kwargs) == expected


@pytest.mark.parametrize("uid", (0, 1000))
def test_is_privileged_tests_against_uid(mocker, uid):
    if not _has_pyprctl:
        pytest.skip()

    user = namedtuple("posix_user", "pw_uid,pw_name")(uid, "asdf")
    mock_prctl = mocker.patch(f"{_MOCK_BASE}_pyprctl")
    mock_prctl.CapState.get_current.return_value.effective = {}

    assert is_privileged(user) is bool(0 == uid)


@pytest.mark.parametrize("uname", ("root", "not_root_uname"))
def test_is_privileged_tests_for_root_username(mocker, uname):
    if not _has_pyprctl:
        pytest.skip()

    user = namedtuple("posix_user", "pw_uid,pw_name")(987, uname)
    mock_prctl = mocker.patch(f"{_MOCK_BASE}_pyprctl")
    mock_prctl.CapState.get_current.return_value.effective = {}

    assert is_privileged(user) is bool("root" == uname)


if _has_pyprctl:

    @pytest.mark.parametrize("cap", ({pyprctl.Cap.SYS_ADMIN}, {}))
    def test_is_privileged_checks_for_privileges(mocker, cap):
        if not _has_pyprctl:
            pytest.skip()

        user = namedtuple("posix_user", "pw_uid,pw_name")(987, "asdf")
        mock_prctl = mocker.patch(f"{_MOCK_BASE}_pyprctl")
        mock_prctl.CapState.get_current.return_value.effective = cap
        assert is_privileged(user) is bool(cap)


@pytest.mark.parametrize(
    "start_url, port, end_url",
    [
        ("amqp://some.domain:1234", 1111, "amqp://some.domain:1111"),
        ("https://domain.com:4567/homepage", 2222, "https://domain.com:2222/homepage"),
        (
            "postgres://user:pass@some.domain:5678",
            3333,
            "postgres://user:pass@some.domain:3333",
        ),
        (
            "postgres://user:pass@some.domain/funcx",
            4444,
            "postgres://user:pass@some.domain:4444/funcx",
        ),
    ],
)
def test_update_url_port(start_url, port, end_url):
    assert update_url_port(start_url, port) == end_url


@pytest.mark.parametrize("err_msg", (None, "asdf"))
def test_cmd_send_failure_publishes_message(mock_mq_chan, randomstring, err_msg):
    mock_exchange_name = randomstring()
    mock_routing_key = randomstring()
    uep_uuid = str(uuid.uuid4())
    amqp_creds = {
        "endpoint_id": uep_uuid,
        "result_queue_info": {
            "connection_url": "abc",
            "queue_publish_kwargs": {
                "exchange": mock_exchange_name,
                "routing_key": mock_routing_key,
            },
        },
        "heartbeat_queue_info": {
            "connection_url": "abc",
            "queue_publish_kwargs": {
                "exchange": mock_exchange_name,
                "routing_key": mock_routing_key,
            },
        },
    }
    send_endpoint_startup_failure_to_amqp(amqp_creds, msg=err_msg)

    assert mock_mq_chan.basic_publish.called
    _a, k = mock_mq_chan.basic_publish.call_args
    assert k["exchange"] == mock_exchange_name
    assert k["routing_key"] == mock_routing_key
    assert k["mandatory"]

    if err_msg is None:
        err_msg = "General or unknown failure starting user endpoint"

    assert uep_uuid.encode() in k["body"]
    assert err_msg.encode() in k["body"]


@pytest.mark.parametrize("preserve", ((15, 16), (3, 5, 6, 11), ()))
def test_all_files_closed(preserve):
    _soft_no, hard_no = resource.getrlimit(resource.RLIMIT_NOFILE)

    with mock.patch(f"{_MOCK_BASE}_os.closerange") as mock_call:
        close_all_fds(preserve)

    not_closed_fds = set()
    closed_ranges = set()
    closed_lows = set()
    closed_highs = set()
    for (low, high), _ in mock_call.call_args_list:
        closed_lows.add(low)
        closed_highs.add(high)
        closed_ranges.add(range(low, high))
        not_closed_fds.add(high)

    for fd in preserve:
        for closed_range in closed_ranges:
            assert fd not in closed_range

    assert any(0 in r for r in closed_ranges), "Expect all but preserved closed"
    assert any(hard_no in r for r in closed_ranges), "Expect all but preserved closed"
