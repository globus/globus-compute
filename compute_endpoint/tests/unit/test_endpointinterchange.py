from concurrent.futures import Future
from unittest import mock

import pytest
from globus_compute_common.messagepack.message_types import EPStatusReport
from globus_compute_common.tasks import TaskState
from globus_compute_endpoint.endpoint.config import UserEndpointConfig
from globus_compute_endpoint.endpoint.interchange import EndpointInterchange
from globus_compute_endpoint.endpoint.rabbit_mq import (
    ResultPublisher,
    TaskQueueSubscriber,
)
from globus_compute_endpoint.engines import GCFuture, GlobusComputeEngine

_mock_base = "globus_compute_endpoint.endpoint.interchange."


def empty_reg_info() -> dict:
    return {
        "task_queue_info": {},
        "result_queue_info": {},
        "heartbeat_queue_info": {},
    }


@pytest.fixture
def mock_log():
    with mock.patch(f"{_mock_base}log") as m:
        yield m


@pytest.fixture
def mock_tqs():
    m = mock.Mock(spec=TaskQueueSubscriber)
    with mock.patch(f"{_mock_base}TaskQueueSubscriber", return_value=m):
        yield m


@pytest.fixture
def mock_rp():
    m = mock.Mock(spec=ResultPublisher)
    with mock.patch(f"{_mock_base}ResultPublisher", return_value=m):
        yield m


@pytest.fixture
def mock_pack():
    with mock.patch(f"{_mock_base}pack") as m:
        yield m


@pytest.fixture
def mock_gce():
    return mock.Mock(spec=GlobusComputeEngine)


@pytest.fixture
def mock_ep_info(randomstring):
    return {
        "some": randomstring(),
        "canary": randomstring(),
        randomstring(): "info",
    }


@pytest.fixture
def ei(endpoint_uuid, mock_gce, mock_quiesce, mock_ep_info):
    _ei = EndpointInterchange(
        config=UserEndpointConfig(engine=mock_gce),
        endpoint_id=endpoint_uuid,
        reg_info=empty_reg_info(),
        ep_info=mock_ep_info,
    )
    _ei._quiesce_event = mock_quiesce
    _ei.engine.get_status_report.return_value = EPStatusReport(
        endpoint_id=_ei.endpoint_id, global_state={}, task_statuses=[]
    )

    yield _ei

    _ei.stop()


@pytest.mark.parametrize("num", list(range(1, 11)))
def test_main_exception_always_quiesces(fs, ei, randomstring, reset_signals, num):
    excepts_left = num
    exc_text = f"Woot {randomstring()}"
    exc = Exception(exc_text)

    def _mock_start_threads(*a, **k):
        nonlocal excepts_left
        excepts_left -= 1
        ei.time_to_quit = excepts_left <= 0
        raise exc

    ei.reconnect_attempt_limit = num + 10
    ei._main_loop = mock.Mock(spec=ei._main_loop)
    ei._main_loop.side_effect = _mock_start_threads
    with mock.patch(f"{_mock_base}time.sleep"):
        with mock.patch(f"{_mock_base}log") as mock_log:
            ei.start()
    assert mock_log.error.call_count == num
    a, _k = mock_log.error.call_args
    assert exc_text in a[0], "Expect exception text shared in logs"

    assert ei._quiesce_event.set.called
    assert ei._quiesce_event.set.call_count == num
    assert ei._main_loop.call_count == num


@pytest.mark.parametrize("attempt_limit", list(range(1, 11)))
def test_reconnect_attempt_limit(fs, ei, attempt_limit, reset_signals):
    expected_reconnect_limit = max(1, attempt_limit)  # checking boundary
    num_iterations = expected_reconnect_limit * 2 + 5  # overkill "just to be sure"
    excepts_left = num_iterations

    ei.reconnect_attempt_limit = attempt_limit
    ei._main_loop = mock.Mock(spec=ei._main_loop)
    with mock.patch(f"{_mock_base}time.sleep"):
        with mock.patch(f"{_mock_base}log") as mock_log:
            ei.start()

    assert ei._main_loop.call_count == expected_reconnect_limit

    expected_msg = f"Failed {expected_reconnect_limit} consecutive"
    assert mock_log.critical.called
    assert expected_msg in mock_log.critical.call_args[0][0]
    assert excepts_left > 0, "expect reconnect limit to stop loop, not test backup"


def test_reset_reconnect_attempt_limit_when_stable(mocker, fs, ei, mock_tqs, mock_pack):
    mocker.patch(f"{_mock_base}ResultPublisher")
    mocker.patch(f"{_mock_base}threading.Thread")

    ei._quiesce_event.wait.side_effect = (False, True)  # iterate once
    ei._reconnect_fail_counter = 1  # simulate that we've failed once
    ei._main_loop()
    assert ei._reconnect_fail_counter == 1, "Expect single iteration to not reset value"
    assert ei._quiesce_event.wait.call_count == 2, "Verify loop iterations"

    # nothing changes about the logic by *just* running again
    ei._quiesce_event.wait.reset_mock()
    ei._quiesce_event.wait.side_effect = (False, True)  # iterate once
    ei._main_loop()
    assert ei._reconnect_fail_counter == 1, "Expect single iteration to not reset value"
    assert ei._quiesce_event.wait.call_count == 2, "Verify loop iterations"

    ei._quiesce_event.wait.reset_mock()
    ei._quiesce_event.wait.side_effect = (False, False, True)
    ei._main_loop()
    assert ei._reconnect_fail_counter == 0, "Expect stable determination after 2 HBs"
    assert ei._quiesce_event.wait.call_count == 3, "Verify loop iterations"


def test_rundir_passed_to_gcengine(mocker, fs, ei):
    "Test EndpointInterchange passing run_dir to GCE on start"

    mocker.patch(f"{_mock_base}ResultPublisher")
    mocker.patch(f"{_mock_base}threading.Thread")
    mock_gcengine = mocker.Mock()
    mock_gcengine.start = mocker.Mock()

    ei.start_engine()

    a, k = ei.engine.start.call_args
    assert k["run_dir"] == ei.logdir


def test_heartbeat_includes_static_info(ei, mock_rp, mock_tqs, mock_pack, mock_ep_info):
    call_count = 0
    num_hbs_until_quit = 2

    def two_hbs_then_quit(*a, **k):
        nonlocal call_count, num_hbs_until_quit
        call_count += 1
        if call_count >= num_hbs_until_quit:
            ei.stop()
        f = Future()
        f.set_result(None)
        return f

    mock_rp.publish.side_effect = two_hbs_then_quit

    with mock.patch(f"{_mock_base}threading.Thread"):
        assert not ei.time_to_quit, "Verify test setup"
        ei._main_loop()

    # Note that this is the "hooked up" test.  The content of `get_status_report`
    # is verified by the engine-specific unit tests.
    assert (
        ei.engine.get_status_report.call_count == num_hbs_until_quit + 1
    ), f"Should be {num_hbs_until_quit} heartbeats and a final sign off"
    for a, k in mock_pack.call_args_list:
        assert all(a[0].global_state[k] == v for k, v in mock_ep_info.items())


@pytest.mark.parametrize("audit_fd", (-1, None))
@pytest.mark.parametrize("is_ha", (True, False))
def test_audit_func_cleared_if_not_ha(mock_gce, task_uuid, audit_fd, is_ha, mock_log):
    conf = UserEndpointConfig(high_assurance=is_ha, engine=mock_gce)
    f = GCFuture(task_uuid)
    ei = EndpointInterchange(
        conf, reg_info=empty_reg_info(), ep_info={}, audit_fd=audit_fd
    )
    assert not ei.time_to_quit

    exp_cleared = not (is_ha and bool(audit_fd))

    ei.audit(TaskState.RECEIVED, f)
    assert ei.time_to_quit is not exp_cleared, "non-cleared invokes self.stop()"
