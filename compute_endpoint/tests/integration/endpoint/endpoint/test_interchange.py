import os
import pathlib
import threading
import uuid
from importlib.machinery import SourceFileLoader

import pytest
from globus_compute_common.messagepack import pack, unpack
from globus_compute_common.messagepack.message_types import EPStatusReport, Result, Task
from globus_compute_endpoint.endpoint.endpoint import Endpoint
from globus_compute_endpoint.endpoint.interchange import EndpointInterchange, log
from globus_compute_endpoint.endpoint.utils.config import Config
from tests.utils import try_for_timeout

_MOCK_BASE = "globus_compute_endpoint.endpoint.interchange."


@pytest.fixture
def funcx_dir(tmp_path):
    fxdir = tmp_path / pathlib.Path("funcx")
    fxdir.mkdir()
    yield fxdir


def test_endpoint_id(funcx_dir):
    manager = Endpoint()
    config_dir = funcx_dir / "mock_endpoint"

    manager.configure_endpoint(config_dir, None)
    endpoint_config = SourceFileLoader(
        "config", str(funcx_dir / "mock_endpoint" / "config.py")
    ).load_module()

    for executor in endpoint_config.config.executors:
        executor.passthrough = False

    ic = EndpointInterchange(
        endpoint_config.config,
        reg_info={"task_queue_info": {}, "result_queue_info": {}},
        endpoint_id="mock_endpoint_id",
    )

    for executor in ic.executors.values():
        assert executor.endpoint_id == "mock_endpoint_id"


def test_start_requires_pre_registered(funcx_dir):
    with pytest.raises(TypeError):
        EndpointInterchange(
            config=Config(),
            reg_info=None,
            endpoint_id="mock_endpoint_id",
        )


def test_invalid_message_result_returned(mocker, endpoint_uuid):
    ei = EndpointInterchange(
        endpoint_id=endpoint_uuid,
        config=Config(executors=[mocker.Mock(endpoint_id=endpoint_uuid)]),
        reg_info={"task_queue_info": {}, "result_queue_info": {}},
    )

    mock_results = mocker.MagicMock()
    mocker.patch(f"{_MOCK_BASE}ResultQueuePublisher", return_value=mock_results)
    mocker.patch(f"{_MOCK_BASE}convert_to_internaltask", side_effect=Exception("BLAR"))
    task = Task(task_id=uuid.uuid4(), task_buffer="")
    ei.pending_task_queue.put(pack(task))
    t = threading.Thread(target=ei._main_loop, daemon=True)
    t.start()

    try_for_timeout(lambda: mock_results.publish.called, timeout_ms=1000)
    ei.time_to_quit = True
    t.join()

    assert mock_results.publish.called
    msg = mock_results.publish.call_args_list[0][0][0]
    result: Result = unpack(msg)
    assert result.task_id == task.task_id
    assert "Failed to start task" in result.data


def test_die_with_parent_refuses_to_start_if_not_parent(mocker):
    ei = EndpointInterchange(
        config=Config(executors=[]),
        reg_info={"task_queue_info": {}, "result_queue_info": {}},
        parent_pid=os.getpid(),  # _not_ ppid; that's the test.
    )
    mock_warn = mocker.patch.object(log, "warning")
    assert not ei._kill_event.is_set()
    ei.start()
    assert ei._kill_event.is_set()

    warn_msg = str(list(a[0] for a, _ in mock_warn.call_args_list))
    assert "refusing to start" in warn_msg


def test_die_with_parent_goes_away_if_parent_dies(mocker):
    ppid = os.getppid()

    mocker.patch(f"{_MOCK_BASE}ResultQueuePublisher")
    mocker.patch(f"{_MOCK_BASE}convert_to_internaltask")
    mocker.patch(f"{_MOCK_BASE}time.sleep")
    mock_ppid = mocker.patch(f"{_MOCK_BASE}os.getppid")
    mock_ppid.side_effect = (ppid, 1)
    ei = EndpointInterchange(
        config=Config(executors=[]),
        reg_info={"task_queue_info": {}, "result_queue_info": {}},
        parent_pid=ppid,
    )
    ei.executors = {"mock_executor": mocker.Mock()}
    mock_warn = mocker.patch.object(log, "warning")
    assert not ei._kill_event.is_set()
    ei.start()
    assert ei._kill_event.is_set()

    warn_msg = str(list(a[0] for a, _ in mock_warn.call_args_list))
    assert "refusing to start" not in warn_msg
    assert f"Parent ({ppid}) has gone away" in warn_msg


def test_no_idle_if_not_configured(mocker, endpoint_uuid):
    mock_spt = mocker.patch(f"{_MOCK_BASE}setproctitle.setproctitle")
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    mocker.patch(f"{_MOCK_BASE}ResultQueuePublisher")

    conf = Config(
        executors=[mocker.Mock(endpoint_id=endpoint_uuid)],
        heartbeat_period=0.01,
        idle_heartbeats_soft=0,
    )
    ei = EndpointInterchange(
        endpoint_id=endpoint_uuid,
        config=conf,
        reg_info={"task_queue_info": {}, "result_queue_info": {}},
    )
    t = threading.Thread(target=ei._main_loop, daemon=True)
    t.start()

    try_for_timeout(lambda: mock_log.debug.call_count > 50, timeout_ms=1000)
    ei.time_to_quit = True
    t.join()
    assert not mock_spt.called


def test_soft_idle_honored(mocker, endpoint_uuid):
    mock_spt = mocker.patch(f"{_MOCK_BASE}setproctitle.setproctitle")
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    mocker.patch(f"{_MOCK_BASE}ResultQueuePublisher")

    conf = Config(
        executors=[mocker.Mock(endpoint_id=endpoint_uuid)],
        heartbeat_period=0.1,
        idle_heartbeats_soft=3,
    )
    ei = EndpointInterchange(
        endpoint_id=endpoint_uuid,
        config=conf,
        reg_info={"task_queue_info": {}, "result_queue_info": {}},
    )
    ei.results_passthrough.put({"task_id": uuid.uuid1(), "message": ""})

    ei._main_loop()

    log_args = [m[0][0] for m in mock_log.info.call_args_list]
    transition_count = sum("In idle state" in m for m in log_args)
    assert transition_count == 1, "expected logs not spammed"

    idle_msg = next(m for m in log_args if "In idle state" in m)
    assert "due to" in idle_msg, "expected to find reason"
    assert "idle_heartbeats_soft" in idle_msg, "expected to find setting name"
    assert " shut down in " in idle_msg, "expected to find timeout time"

    idle_msg = next(m for m in log_args if "Idle heartbeats reached." in m)
    assert "Shutting down" in idle_msg, "expected to find action taken"

    num_updates = sum(
        m[0][0].startswith("[idle; shut down in ") for m in mock_spt.call_args_list
    )
    assert num_updates > 1, "expect process title reflects idle status and is updated"


def test_hard_idle_honored(mocker, endpoint_uuid):
    mock_spt = mocker.patch(f"{_MOCK_BASE}setproctitle.setproctitle")
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    mocker.patch(f"{_MOCK_BASE}ResultQueuePublisher")

    conf = Config(
        executors=[mocker.Mock(endpoint_id=endpoint_uuid)],
        heartbeat_period=0.1,
        idle_heartbeats_soft=1,
        idle_heartbeats_hard=3,
    )
    ei = EndpointInterchange(
        endpoint_id=endpoint_uuid,
        config=conf,
        reg_info={"task_queue_info": {}, "result_queue_info": {}},
    )
    ei._main_loop()

    log_args = [m[0][0] for m in mock_log.info.call_args_list]
    transition_count = sum("Possibly idle" in m for m in log_args)
    assert transition_count == 1, "expected logs not spammed"

    idle_msg = next(m for m in log_args if "Possibly idle" in m)
    assert "idle_heartbeats_hard" in idle_msg, "expected to find setting name"
    assert " shut down in " in idle_msg, "expected to find timeout time"

    idle_msg = mock_log.warning.call_args[0][0]
    assert "Shutting down" in idle_msg, "expected to find action taken"
    assert "HARD limit" in idle_msg

    num_updates = sum(
        m[0][0].startswith("[possibly idle; shut down in ")
        for m in mock_spt.call_args_list
    )
    assert num_updates > 1, "expect process title reflects idle status and is updated"


def test_unidle_updates_proc_title(mocker, endpoint_uuid):
    mock_spt = mocker.patch(f"{_MOCK_BASE}setproctitle.setproctitle")
    mock_log = mocker.patch(f"{_MOCK_BASE}log")
    mocker.patch(f"{_MOCK_BASE}ResultQueuePublisher")

    conf = Config(
        executors=[mocker.Mock(endpoint_id=endpoint_uuid)],
        heartbeat_period=0.1,
        idle_heartbeats_soft=1,
        idle_heartbeats_hard=3,
    )
    ei = EndpointInterchange(
        endpoint_id=endpoint_uuid,
        config=conf,
        reg_info={"task_queue_info": {}, "result_queue_info": {}},
    )

    def insert_msg():
        ei.results_passthrough.put({"task_id": uuid.uuid1(), "message": ""})
        while True:
            yield

    mock_spt.side_effect = insert_msg()

    ei._main_loop()

    msg = next(m[0][0] for m in mock_log.info.call_args_list if "Moved to" in m[0][0])
    assert msg.startswith("Moved to active state"), "expect why state changed"
    assert "due to " in msg

    first, second, third = (ca[0][0] for ca in mock_spt.call_args_list)
    assert first.startswith("[possibly idle; shut down in ")
    assert "idle; " not in second, "expected proc title set back when not idle"
    assert third.startswith("[idle; shut down in ")


def test_sends_final_status_message_on_shutdown(mocker, endpoint_uuid):
    mocker.patch(f"{_MOCK_BASE}setproctitle.setproctitle")
    mock_rqp = mocker.MagicMock()
    mocker.patch(f"{_MOCK_BASE}ResultQueuePublisher", return_value=mock_rqp)

    conf = Config(
        executors=[mocker.Mock(endpoint_id=endpoint_uuid)],
        heartbeat_period=0.01,
        idle_heartbeats_soft=1,
        idle_heartbeats_hard=2,
    )
    ei = EndpointInterchange(
        endpoint_id=endpoint_uuid,
        config=conf,
        reg_info={"task_queue_info": {}, "result_queue_info": {}},
    )
    ei._main_loop()

    assert mock_rqp.publish.called
    packed_bytes = mock_rqp.publish.call_args[0][0]
    epsr = unpack(packed_bytes)
    assert isinstance(epsr, EPStatusReport)
    assert epsr.endpoint_id == uuid.UUID(endpoint_uuid)
    assert epsr.global_state["heartbeat_period"] == 0
