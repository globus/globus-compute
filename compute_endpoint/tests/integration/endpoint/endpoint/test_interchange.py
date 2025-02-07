import os
import queue
import random
import threading
import typing as t
import uuid
from concurrent.futures import Future, TimeoutError
from unittest import mock

import pytest
from globus_compute_common.messagepack import pack, unpack
from globus_compute_common.messagepack.message_types import EPStatusReport, Result
from globus_compute_endpoint import engines
from globus_compute_endpoint.endpoint.config.config import UserEndpointConfig
from globus_compute_endpoint.endpoint.interchange import EndpointInterchange, log
from globus_compute_endpoint.endpoint.rabbit_mq import (
    ResultPublisher,
    TaskQueueSubscriber,
)
from globus_compute_endpoint.engines import GlobusComputeEngine
from tests.utils import try_assert

_MOCK_BASE = "globus_compute_endpoint.endpoint.interchange."


@pytest.fixture
def gc_dir(tmp_path):
    fxdir = tmp_path / ".globus_compute"
    fxdir.mkdir()
    yield fxdir


@pytest.fixture(autouse=True)
def reset_signals_auto(reset_signals):
    yield


@pytest.fixture
def mock_log(mocker):
    yield mocker.patch(f"{_MOCK_BASE}log")


@pytest.fixture
def mock_engine(endpoint_uuid):
    ex = mock.Mock(spec=GlobusComputeEngine, endpoint_id=endpoint_uuid)
    ex.get_status_report.return_value = EPStatusReport(
        endpoint_id=endpoint_uuid, global_state={}, task_statuses=[]
    )
    ex.executor_exception = None
    yield ex


@pytest.fixture
def mock_conf(mock_engine):
    yield UserEndpointConfig(engine=mock_engine)


@pytest.fixture
def mock_rp():
    m = mock.Mock(spec=ResultPublisher)
    with mock.patch(f"{_MOCK_BASE}ResultPublisher", return_value=m):
        yield m


@pytest.fixture
def mock_tqs():
    m = mock.Mock(spec=TaskQueueSubscriber)
    with mock.patch(f"{_MOCK_BASE}TaskQueueSubscriber", return_value=m):
        yield m


@pytest.fixture
def mock_pack():
    with mock.patch(f"{_MOCK_BASE}pack") as m:
        yield m


@pytest.fixture
def ep_ix_factory(endpoint_uuid, mock_conf, mock_quiesce):
    to_stop: t.List[EndpointInterchange] = []

    def _f(*a, **k):
        reg_info = {
            "task_queue_info": {},
            "result_queue_info": {},
            "heartbeat_queue_info": {},
        }
        kw = {
            "endpoint_id": endpoint_uuid,
            "config": mock_conf,
            "reg_info": reg_info,
            "ep_info": {},
        }
        kw.update(k)
        ei = EndpointInterchange(*a, **kw)
        ei._quiesce_event = mock_quiesce
        to_stop.append(ei)
        return ei

    yield _f

    for _ei in to_stop:
        _ei.stop()


@pytest.fixture
def ep_ix(ep_ix_factory):
    yield ep_ix_factory()


@pytest.fixture(autouse=True)
def mock_spt(mocker):
    yield mocker.patch(f"{_MOCK_BASE}setproctitle.setproctitle")


def test_endpoint_id_conveyed_to_engine(gc_dir, mock_conf, ep_uuid):
    mock_conf.engine = engines.ThreadPoolEngine()
    ic = EndpointInterchange(
        mock_conf,
        reg_info={
            "task_queue_info": {},
            "result_queue_info": {},
            "heartbeat_queue_info": {},
        },
        ep_info={},
        endpoint_id=ep_uuid,
    )
    ic.start_engine()
    assert ic.engine.endpoint_id == ep_uuid
    ic.engine.shutdown(block=True)


def test_start_requires_pre_registered(mock_conf, gc_dir):
    with pytest.raises(TypeError):
        EndpointInterchange(
            config=mock_conf, reg_info=None, ep_info={}, endpoint_id="mock_endpoint_id"
        )


@pytest.mark.parametrize("num_iters", (1, 2, 5, 10))
def test_detects_bad_executor_when_no_tasks(
    mock_log, num_iters, ep_ix, mock_engine, randomstring, mock_rp, mock_tqs
):
    expected_iters = num_iters
    exc_text = randomstring()

    def update_executor_exception(*_a, **_k):
        nonlocal num_iters
        num_iters -= 1
        if not num_iters:
            mock_engine.executor_exception = Exception(exc_text)
        raise queue.Empty

    with mock.patch.object(ep_ix, "pending_task_queue") as ptq:
        ptq.get.side_effect = update_executor_exception

        ep_ix._main_loop()

        assert ep_ix.time_to_quit, "Sanity check"
        assert ep_ix.pending_task_queue.get.call_count == expected_iters
    loglines = "\n".join(str(a[0]) for a, _k in mock_log.exception.call_args_list)
    assert exc_text in loglines, "Expected faithful sharing of executor exception"


def test_die_with_parent_refuses_to_start_if_not_parent(mocker, ep_ix_factory):
    ei = ep_ix_factory(parent_pid=os.getpid())  # _not_ ppid; that's the test.
    mock_warn = mocker.patch.object(log, "warning")
    assert not ei.time_to_quit, "Verify test setup"
    ei.start()
    assert ei.time_to_quit

    warn_msg = str(list(a[0] for a, _ in mock_warn.call_args_list))
    assert "refusing to start" in warn_msg


def test_die_with_parent_goes_away_if_parent_dies(mocker, ep_ix_factory, mock_rp):
    ppid = os.getppid()

    mocker.patch(f"{_MOCK_BASE}time.sleep")
    mock_ppid = mocker.patch(f"{_MOCK_BASE}os.getppid")
    mock_ppid.side_effect = (ppid, 1)
    ei = ep_ix_factory(parent_pid=ppid)
    mock_warn = mocker.patch.object(log, "warning")
    assert not ei.time_to_quit, "Verify test setup"

    ei.start()
    assert ei.time_to_quit

    warn_msg = str(list(a[0] for a, _ in mock_warn.call_args_list))
    assert "refusing to start" not in warn_msg
    assert f"Parent ({ppid}) has gone away" in warn_msg


def test_no_idle_if_not_configured(
    mocker,
    ep_ix_factory,
    mock_conf,
    mock_log,
    endpoint_uuid,
    mock_spt,
    mock_quiesce,
    mock_rp,
    mock_tqs,
    mock_pack,
):
    mock_conf.idle_heartbeats_soft = 0
    mock_conf.heartbeat_period = 1
    ei = ep_ix_factory(config=mock_conf)
    ei.results_passthrough = mocker.Mock(spec=queue.Queue)
    ei.results_passthrough.get.side_effect = queue.Empty
    ei.pending_task_queue = mocker.Mock(spec=queue.SimpleQueue)
    ei.pending_task_queue.get.side_effect = queue.Empty
    ei._quiesce_event = mock_quiesce

    t = threading.Thread(target=ei._main_loop, daemon=True)
    t.start()

    try_assert(lambda: mock_log.debug.call_count > 500)
    ei.time_to_quit = True
    t.join()
    assert not mock_spt.called


@pytest.mark.parametrize("idle_limit", (random.randint(2, 100),))
def test_soft_idle_honored(
    mocker,
    mock_log,
    mock_conf,
    ep_ix_factory,
    mock_spt,
    idle_limit,
    mock_quiesce,
    mock_rp,
    mock_tqs,
    mock_pack,
):
    result = Result(task_id=uuid.uuid1(), data=b"TASK RESULT")
    msg = {"task_id": str(result.task_id), "message": pack(result)}

    mock_conf.idle_heartbeats_soft = idle_limit
    ei = ep_ix_factory(config=mock_conf)

    ei.results_passthrough = mocker.Mock(spec=queue.Queue)
    ei.results_passthrough.get.side_effect = (msg, queue.Empty)
    ei.pending_task_queue = mocker.Mock(spec=queue.SimpleQueue)
    ei.pending_task_queue.get.side_effect = queue.Empty

    ei._quiesce_event = mock_quiesce
    ei._main_loop()

    assert ei.time_to_quit is True

    log_args = [a[0] for a, _k in mock_log.info.call_args_list]
    transition_count = sum("In idle state" in m for m in log_args)
    assert transition_count == 1, f"expected logs not spammed -- {log_args}"

    shut_down_s = f"{(idle_limit - 1) * mock_conf.heartbeat_period:,}"
    idle_msg = next(m for m in log_args if "In idle state" in m)
    assert "due to" in idle_msg, "expected to find reason"
    assert "idle_heartbeats_soft" in idle_msg, "expected to find setting name"
    assert f" shut down in {shut_down_s}" in idle_msg, "expected to find timeout time"

    idle_msg = next(m for m in log_args if "Idle heartbeats reached." in m)
    assert "Shutting down" in idle_msg, "expected to find action taken"

    num_updates = sum(
        m[0][0].startswith("[idle; shut down in ") for m in mock_spt.call_args_list
    )
    assert num_updates == idle_limit, "expect process title updated; reflects status"


@pytest.mark.parametrize("idle_limit", (random.randint(4, 100),))
def test_hard_idle_honored(
    mocker,
    mock_log,
    mock_conf,
    ep_ix_factory,
    mock_spt,
    idle_limit,
    mock_quiesce,
    mock_rp,
    mock_tqs,
    mock_pack,
):
    idle_soft_limit = random.randrange(2, idle_limit)

    mocker.patch(f"{_MOCK_BASE}threading.Thread")

    mock_conf.idle_heartbeats_soft = idle_soft_limit
    mock_conf.idle_heartbeats_hard = idle_limit
    ei = ep_ix_factory(config=mock_conf)
    ei._quiesce_event = mock_quiesce

    ei._main_loop()

    log_args = [m[0][0] for m in mock_log.info.call_args_list]
    transition_count = sum("Possibly idle" in m for m in log_args)
    assert transition_count == 1, "expected logs not spammed"

    shut_down_s = f"{(idle_limit - idle_soft_limit - 1) * mock_conf.heartbeat_period:,}"
    idle_msg = next(m for m in log_args if "Possibly idle" in m)
    assert "idle_heartbeats_hard" in idle_msg, "expected to find setting name"
    assert f" shut down in {shut_down_s}" in idle_msg, "expected to find timeout time"

    idle_msg = mock_log.warning.call_args[0][0]
    assert "Shutting down" in idle_msg, "expected to find action taken"
    assert "HARD limit" in idle_msg

    num_updates = sum(
        m[0][0].startswith("[possibly idle; shut down in ")
        for m in mock_spt.call_args_list
    )
    assert (
        num_updates == idle_limit - idle_soft_limit
    ), "expect process title updated; reflects status"


def test_unidle_updates_proc_title(
    mocker,
    mock_log,
    mock_conf,
    ep_ix_factory,
    mock_spt,
    mock_quiesce,
    mock_rp,
    mock_tqs,
    mock_pack,
):
    mock_conf.heartbeat_period = 1
    mock_conf.idle_heartbeats_soft = 1
    mock_conf.idle_heartbeats_hard = 3
    ei = ep_ix_factory(config=mock_conf)
    ei._quiesce_event = mock_quiesce
    ei.results_passthrough = mocker.Mock(spec=queue.Queue)
    ei.results_passthrough.get.side_effect = queue.Empty
    ei.pending_task_queue = mocker.Mock(spec=queue.SimpleQueue)
    ei.pending_task_queue.get.side_effect = queue.Empty

    main_thread_may_continue = threading.Event()

    def return_msg_set_empty():
        result = Result(task_id=uuid.uuid1(), data=b"TASK RESULT")
        yield {"task_id": str(result.task_id), "message": pack(result)}
        main_thread_may_continue.set()
        ei.results_passthrough.get.side_effect = queue.Empty
        raise queue.Empty

    def insert_msg(*a, **k):
        ei.results_passthrough.get.side_effect = return_msg_set_empty()
        main_thread_may_continue.wait()

    mock_spt.side_effect = insert_msg

    ei._main_loop()

    msg = next(m[0][0] for m in mock_log.info.call_args_list if "Moved to" in m[0][0])
    assert msg.startswith("Moved to active state"), "expect why state changed"
    assert "due to " in msg

    first, *middle, last = (ca[0][0] for ca in mock_spt.call_args_list)
    assert first.startswith("[possibly idle; shut down in ")
    assert not any(
        "idle; " in m for m in middle
    ), "expected proc title set back when not idle"
    assert last.startswith("[idle; shut down in ")


def test_sends_final_status_message_on_shutdown(
    mocker,
    mock_log,
    mock_conf,
    ep_ix_factory,
    endpoint_uuid,
    mock_quiesce,
    mock_rp,
    mock_tqs,
):
    mock_conf.idle_heartbeats_soft = 1
    mock_conf.idle_heartbeats_hard = 2
    ei = ep_ix_factory(config=mock_conf)
    ei.results_passthrough = mocker.Mock(spec=queue.Queue)
    ei.results_passthrough.get.side_effect = queue.Empty
    ei.pending_task_queue = mocker.Mock(spec=queue.SimpleQueue)
    ei.pending_task_queue.get.side_effect = queue.Empty
    ei._quiesce_event = mock_quiesce
    ei._main_loop()

    assert mock_rp.publish.called
    packed_bytes = mock_rp.publish.call_args[0][0]
    epsr = unpack(packed_bytes)
    assert isinstance(epsr, EPStatusReport)
    assert epsr.endpoint_id == uuid.UUID(endpoint_uuid)
    assert epsr.global_state["active"] is False


def test_gracefully_handles_final_status_message_timeout(
    mocker,
    mock_log,
    mock_conf,
    ep_ix_factory,
    endpoint_uuid,
    mock_quiesce,
    mock_rp,
    mock_tqs,
    mock_pack,
):
    mock_conf.idle_heartbeats_soft = 1
    mock_conf.idle_heartbeats_hard = 2
    ei = ep_ix_factory(config=mock_conf)
    ei.results_passthrough = mocker.Mock(spec=queue.Queue)
    ei.results_passthrough.get.side_effect = queue.Empty
    ei.pending_task_queue = mocker.Mock(spec=queue.SimpleQueue)
    ei.pending_task_queue.get.side_effect = queue.Empty
    ei._quiesce_event = mock_quiesce

    mock_future = mocker.Mock(spec=Future)
    mock_future.result.side_effect = TimeoutError("asdfasdf")
    mock_rp.publish.return_value = mock_future
    ei._main_loop()

    assert mock_rp


def test_sends_status_reports(
    mocker, ep_ix, endpoint_uuid, randomstring, mock_quiesce, mock_rp, mock_tqs
):
    status_reports = [
        EPStatusReport(endpoint_id=uuid.uuid4(), global_state={}, task_statuses=[])
        for i in range(5)
    ]
    ep_ix.engine.get_status_report.side_effect = status_reports
    ep_ix.config.idle_heartbeats_soft = 1
    ep_ix.config.idle_heartbeats_hard = 0  # will be set to soft + 1
    ep_ix._quiesce_event = mock_quiesce
    num_hbs = ep_ix.config.idle_heartbeats_soft + 4
    with mock.patch(f"{_MOCK_BASE}threading.Thread"):
        ep_ix._main_loop()
    assert mock_rp.publish.call_count == num_hbs, "pre/post-loop(2) + hard idle(3)"

    for exp_sr, (a, _) in zip(status_reports, mock_rp.publish.call_args_list):
        found_sr: EPStatusReport = unpack(a[0])
        assert isinstance(found_sr, EPStatusReport)
        assert found_sr.endpoint_id == exp_sr.endpoint_id
        assert "active" in found_sr.global_state


def test_epi_stored_results_processed(ep_ix_factory, tmp_path, mock_rp, mock_tqs):
    tid = str(uuid.uuid4())
    ep_ix: EndpointInterchange = ep_ix_factory(endpoint_dir=tmp_path)
    ep_ix.result_store[tid] = b"GIBBERISH"

    unacked_results_dir = tmp_path / "unacked_results"
    res_f = unacked_results_dir / tid
    assert res_f.exists(), "Ensure file exists beforehand"

    def _pub(packed_message):
        if packed_message == b"GIBBERISH":
            ep_ix.stop()
        f = Future()
        f.set_result(None)
        return f

    mock_rp.publish.side_effect = _pub

    ep_ix.pending_task_queue = mock.Mock()
    ep_ix.pending_task_queue.get.side_effect = queue.Empty
    ep_ix._main_loop()
    assert not res_f.exists()
