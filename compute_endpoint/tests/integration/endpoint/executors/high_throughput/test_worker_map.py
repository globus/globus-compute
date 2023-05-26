import logging
import os

from globus_compute_endpoint.engines.high_throughput.worker_map import WorkerMap


class TestWorkerMap:
    def test_add_worker(self, mocker):
        mock_popen = mocker.patch(
            "globus_compute_endpoint.engines.high_throughput.worker_map.subprocess.Popen"  # noqa: E501
        )
        mock_popen.return_value = "proc"

        # Test adding with no accelerators
        worker_map = WorkerMap(1, [])
        worker = worker_map.add_worker(
            worker_id="0",
            address="127.0.0.1",
            debug=logging.DEBUG,
            uid="test1",
            logdir=os.getcwd(),
            worker_port=50001,
        )

        assert list(worker.keys()) == ["0"]
        assert worker["0"] == "proc"
        assert worker_map.worker_id_counter == 1
        assert worker_map.available_accelerators is None

        # Test with an accelerator
        worker_map = WorkerMap(1, ["0"])
        worker_map.add_worker(
            worker_id="1",
            address="127.0.0.1",
            debug=logging.DEBUG,
            uid="test1",
            logdir=os.getcwd(),
            worker_port=50001,
        )

        last_call = mock_popen.mock_calls[-1]
        assert last_call[-1]["env"]["CUDA_VISIBLE_DEVICES"] == "0"
