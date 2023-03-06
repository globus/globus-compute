import random

import pytest
from globus_compute_endpoint.endpoint.interchange import EndpointInterchange
from globus_compute_endpoint.endpoint.utils.config import Config

_mock_base = "globus_compute_endpoint.endpoint.interchange."


def test_main_exception_always_quiesces(mocker, fs, reset_signals):
    num_iterations = random.randint(1, 10)

    # [False, False] * num because _kill_event and _quiesce_event are the same mock;
    #  .is_set() is called twice per loop
    is_set_returns = [False, False, False] * num_iterations + [True]
    false_true_g = iter(is_set_returns)

    def false_true():
        return next(false_true_g)

    mocker.patch(f"{_mock_base}multiprocessing")
    ei = EndpointInterchange(
        config=Config(executors=[mocker.Mock()]),
        reg_info={"task_queue_info": {}, "result_queue_info": {}},
        reconnect_attempt_limit=num_iterations + 10,
    )
    ei._task_puller_proc = mocker.MagicMock()
    ei._start_threads_and_main = mocker.MagicMock()
    ei._start_threads_and_main.side_effect = Exception("Woot")
    ei._kill_event.is_set = false_true
    ei.start()

    assert ei._quiesce_event.set.called
    assert ei._quiesce_event.set.call_count == num_iterations
    assert ei._start_threads_and_main.call_count == num_iterations


@pytest.mark.parametrize("reconnect_attempt_limit", [-1, 0, 1, 2, 3, 5, 10])
def test_reconnect_attempt_limit(mocker, fs, reconnect_attempt_limit, reset_signals):
    num_iterations = reconnect_attempt_limit * 2 + 5  # overkill "just to be sure"

    # [False, False] * num because _kill_event and _quiesce_event are the same mock;
    #  .is_set() is called twice per loop
    is_set_returns = [False, False] * num_iterations + [True]
    false_true_g = iter(is_set_returns)

    def false_true():
        return next(false_true_g)

    mocker.patch(f"{_mock_base}multiprocessing")
    mock_log = mocker.patch(f"{_mock_base}log")
    ei = EndpointInterchange(
        config=Config(executors=[mocker.Mock()]),
        reg_info={"task_queue_info": {}, "result_queue_info": {}},
        reconnect_attempt_limit=reconnect_attempt_limit,
    )
    ei._task_puller_proc = mocker.MagicMock()
    ei._start_threads_and_main = mocker.MagicMock()
    ei._start_threads_and_main.side_effect = Exception("Woot")
    ei._kill_event.is_set = false_true
    ei.start()

    reconnect_attempt_limit = max(1, reconnect_attempt_limit)  # checking boundary
    assert ei._start_threads_and_main.call_count == reconnect_attempt_limit

    expected_msg = f"Failed {reconnect_attempt_limit} consecutive"
    assert mock_log.critical.called
    assert expected_msg in mock_log.critical.call_args[0][0]
