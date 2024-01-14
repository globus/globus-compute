import random
from unittest import mock

import pytest
from globus_compute_endpoint.endpoint.config import Config
from globus_compute_endpoint.endpoint.interchange import EndpointInterchange

_mock_base = "globus_compute_endpoint.endpoint.interchange."


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
        config=Config(executors=[mocker.Mock()]),
        reg_info={"task_queue_info": {}, "result_queue_info": {}},
        reconnect_attempt_limit=num_iterations + 10,
    )
    ei._task_puller_proc = mocker.Mock()
    ei._start_threads_and_main = mocker.Mock()
    ei._start_threads_and_main.side_effect = _mock_start_threads
    with mock.patch(f"{_mock_base}log") as mock_log:
        ei.start()
    assert mock_log.error.call_count == num_iterations
    a, _k = mock_log.error.call_args
    assert exc_text in a[0], "Expect exception text shared in logs"

    assert ei._quiesce_event.set.called
    assert ei._quiesce_event.set.call_count == num_iterations
    assert ei._start_threads_and_main.call_count == num_iterations


@pytest.mark.parametrize("reconnect_attempt_limit", [-1, 0, 1, 2, 3, 5, 10])
def test_reconnect_attempt_limit(mocker, fs, reconnect_attempt_limit, reset_signals):
    num_iterations = reconnect_attempt_limit * 2 + 5  # overkill "just to be sure"
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
        config=Config(executors=[mocker.Mock()]),
        reg_info={"task_queue_info": {}, "result_queue_info": {}},
        reconnect_attempt_limit=reconnect_attempt_limit,
    )
    ei._task_puller_proc = mocker.MagicMock()
    ei._start_threads_and_main = mocker.MagicMock()
    ei._start_threads_and_main.side_effect = _mock_start_threads
    ei.start()

    reconnect_attempt_limit = max(1, reconnect_attempt_limit)  # checking boundary
    assert ei._start_threads_and_main.call_count == reconnect_attempt_limit

    expected_msg = f"Failed {reconnect_attempt_limit} consecutive"
    assert mock_log.critical.called
    assert expected_msg in mock_log.critical.call_args[0][0]
    assert excepts_left > 0, "expect reconnect limit to stop loop, not test backup"
