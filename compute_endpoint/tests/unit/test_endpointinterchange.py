import logging
import random
import uuid
from multiprocessing.synchronize import Event as EventType
from unittest import mock

import pytest
from globus_compute_endpoint.endpoint.config import UserEndpointConfig
from globus_compute_endpoint.endpoint.interchange import EndpointInterchange
from globus_compute_endpoint.endpoint.rabbit_mq import TaskQueueSubscriber

_mock_base = "globus_compute_endpoint.endpoint.interchange."


@pytest.fixture
def mock_tqs():
    m = mock.Mock(spec=TaskQueueSubscriber)
    with mock.patch(f"{_mock_base}TaskQueueSubscriber", return_value=m):
        yield m


def test_main_exception_always_quiesces(mocker, fs, randomstring, reset_signals):
    num_iterations = random.randint(1, 10)
    excepts_left = num_iterations
    exc_text = f"Woot {randomstring()}"
    exc = Exception(exc_text)

    def _mock_start_threads(*a, **k):
        nonlocal excepts_left
        excepts_left -= 1
        ei.time_to_quit = excepts_left <= 0
        raise exc

    mocker.patch(f"{_mock_base}time.sleep")
    mocker.patch(f"{_mock_base}multiprocessing")
    ei = EndpointInterchange(
        config=UserEndpointConfig(executors=[mocker.Mock()]),
        reg_info={"task_queue_info": {}, "result_queue_info": {}},
        reconnect_attempt_limit=num_iterations + 10,
    )
    ei._main_loop = mock.Mock()
    ei._main_loop.side_effect = _mock_start_threads
    with mock.patch(f"{_mock_base}log") as mock_log:
        ei.start()
    assert mock_log.error.call_count == num_iterations
    a, _k = mock_log.error.call_args
    assert exc_text in a[0], "Expect exception text shared in logs"

    assert ei._quiesce_event.set.called
    assert ei._quiesce_event.set.call_count == num_iterations
    assert ei._main_loop.call_count == num_iterations


@pytest.mark.parametrize("reconnect_attempt_limit", [-1, 0, 1, 2, 3, 5, 10])
def test_reconnect_attempt_limit(mocker, fs, reconnect_attempt_limit, reset_signals):
    expected_reconnect_limit = max(1, reconnect_attempt_limit)  # checking boundary
    num_iterations = expected_reconnect_limit * 2 + 5  # overkill "just to be sure"
    excepts_left = num_iterations

    def _mock_start_threads(*a, **k):
        nonlocal excepts_left
        excepts_left -= 1
        ei.time_to_quit = excepts_left <= 0
        raise Exception()

    mocker.patch(f"{_mock_base}time.sleep")
    mocker.patch(f"{_mock_base}multiprocessing")
    mock_log = mocker.patch(f"{_mock_base}log")
    ei = EndpointInterchange(
        config=UserEndpointConfig(executors=[mocker.Mock()]),
        reg_info={"task_queue_info": {}, "result_queue_info": {}},
        reconnect_attempt_limit=reconnect_attempt_limit,
    )
    ei._main_loop = mock.Mock(spec=ei._main_loop)
    ei.start()

    assert ei._main_loop.call_count == expected_reconnect_limit

    expected_msg = f"Failed {expected_reconnect_limit} consecutive"
    assert mock_log.critical.called
    assert expected_msg in mock_log.critical.call_args[0][0]
    assert excepts_left > 0, "expect reconnect limit to stop loop, not test backup"


def test_reset_reconnect_attempt_limit_when_stable(mocker, fs, mock_tqs):
    mocker.patch(f"{_mock_base}ResultPublisher")
    mocker.patch(f"{_mock_base}threading.Thread")
    mocker.patch(f"{_mock_base}multiprocessing")
    ei = EndpointInterchange(
        config=UserEndpointConfig(executors=[mocker.Mock()]),
        endpoint_id=str(uuid.uuid4()),
        reg_info={"task_queue_info": {}, "result_queue_info": {}},
    )
    ei._quiesce_event = mock.Mock(spec=EventType)

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


def test_rundir_passed_to_gcengine(mocker, fs):
    "Test EndpointInterchange passing run_dir to GCE on start"

    mocker.patch(f"{_mock_base}ResultPublisher")
    mocker.patch(f"{_mock_base}threading.Thread")
    mocker.patch(f"{_mock_base}multiprocessing")
    mock_gcengine = mocker.Mock()
    mock_gcengine.start = mocker.Mock()

    ei = EndpointInterchange(
        config=UserEndpointConfig(executors=[mock_gcengine]),
        endpoint_id=str(uuid.uuid4()),
        reg_info={"task_queue_info": {}, "result_queue_info": {}},
    )
    ei._quiesce_event = mock.Mock(spec=EventType)

    logging.warning(f"{ei.executor=}")
    ei.start_engine()

    mock_gcengine.start.assert_called_with(
        results_passthrough=ei.results_passthrough,
        endpoint_id=ei.endpoint_id,
        run_dir=ei.logdir,
    )
