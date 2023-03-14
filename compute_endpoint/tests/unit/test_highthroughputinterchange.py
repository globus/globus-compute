import time
import uuid
from unittest import mock

import pytest
from globus_compute_common.tasks import TaskState
from globus_compute_endpoint.executors.high_throughput.interchange import (
    Interchange,
    starter,
)
from globus_compute_endpoint.executors.high_throughput.messages import Task

# Work with linter's 88 char limit, and be uniform in this file how we do it
mod_dot_path = "globus_compute_endpoint.executors.high_throughput.interchange"


@mock.patch(f"{mod_dot_path}.zmq")
@mock.patch(f"{mod_dot_path}.Interchange.load_config")
class TestHighThroughputInterchange:
    def test_migrate_internal_task_status(self, _mzmq, _mfn_conf, tmp_path, mocker):
        mock_evt = mock.Mock()
        mock_evt.is_set.side_effect = [False, True]  # run once, please
        task_id = str(uuid.uuid4())
        packed_task = Task(task_id, "RAW", b"").pack()

        ix = Interchange(logdir=tmp_path, worker_ports=(1, 1))
        ix.task_incoming.recv.return_value = packed_task
        ix.migrate_tasks_to_internal(mock_evt)

        assert task_id in ix.task_status_deltas

        tt = ix.task_status_deltas[task_id][0]
        assert 0 <= time.time_ns() - tt.timestamp < 2000000000, "Expecting a timestamp"
        assert tt.state == TaskState.WAITING_FOR_NODES

    def test_start_task_status(self, _mzmq, _mfn_conf, tmp_path, mocker):
        mock_evt = mock.Mock()
        mock_evt.is_set.side_effect = [False, False, True]  # run once, please
        task_id = str(uuid.uuid4())

        mocker.patch(f"{mod_dot_path}.log")
        mock_thread = mocker.patch(f"{mod_dot_path}.threading")
        mock_thread.Event.return_value = mock_evt

        mock_dispatch = mocker.patch(f"{mod_dot_path}.naive_interchange_task_dispatch")
        mock_dispatch.return_value = ({"mgr": [{"task_id": task_id}]}, 0)

        ix = Interchange(logdir=tmp_path, worker_ports=(1, 1))
        ix.strategy = mock.Mock()
        ix.start()

        assert task_id in ix.task_status_deltas

        tt = ix.task_status_deltas[task_id][0]
        assert 0 <= time.time_ns() - tt.timestamp < 2000000000, "Expecting a timestamp"
        assert tt.state == TaskState.WAITING_FOR_LAUNCH


def test_starter_sends_sentinel_upon_error(mocker):
    q = mocker.Mock()
    mock_ix = mocker.patch(f"{mod_dot_path}.Interchange")
    mock_ix.side_effect = ArithmeticError
    with pytest.raises(ArithmeticError):
        starter(q)
    q.put.assert_called()
    q.close.assert_called()
    q.join_thread.assert_called()
