from unittest.mock import MagicMock

from funcx_endpoint.endpoint.interchange import EndpointInterchange


def test_main_exception_always_quiesces(mocker, tmp_path):
    false_true_g = iter((False, False, True))

    def false_true():
        return next(false_true_g)

    mock_conf = MagicMock()
    mocker.patch("funcx_endpoint.endpoint.interchange.FuncXClient")
    mocker.patch("funcx_endpoint.endpoint.interchange.multiprocessing")
    mocker.patch("funcx_endpoint.endpoint.interchange.mpQueue")
    ei = EndpointInterchange(
        config=mock_conf,
        funcx_client=mocker.Mock(),
        reg_info={"task_queue_info": {}, "result_queue_info": {}},
        logdir=str(tmp_path),
        endpoint_dir=str(tmp_path),
    )
    ei._task_puller_proc = MagicMock()
    ei._start_threads_and_main = MagicMock()
    ei._start_threads_and_main.side_effect = Exception("Woot")
    ei._kill_event.is_set = false_true
    ei.start()

    ei._quiesce_event.set.assert_called()
