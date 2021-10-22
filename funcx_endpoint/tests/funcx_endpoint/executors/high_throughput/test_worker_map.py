import logging
import os

from funcx_endpoint.executors.high_throughput.worker_map import WorkerMap


class TestWorkerMap:
    def test_add_worker(self, mocker):
        mock_popen = mocker.patch(
            "funcx_endpoint.executors.high_throughput.worker_map.subprocess.Popen"
        )
        mock_popen.return_value = "proc"

        worker_map = WorkerMap(1)
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
