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
from globus_compute_common.messagepack.message_types import EPStatusReport, Task
from globus_compute_common.tasks import TaskState
from globus_compute_endpoint import engines
from globus_compute_endpoint.endpoint.config.config import UserEndpointConfig
from globus_compute_endpoint.endpoint.interchange import EndpointInterchange, log
from globus_compute_endpoint.endpoint.rabbit_mq import (
    ResultPublisher,
    TaskQueueSubscriber,
)
from tests.integration.endpoint.executors.mock_executors import MockEngine
from tests.utils import double, try_assert

_MOCK_BASE = "globus_compute_endpoint.endpoint.interchange."
_test_func_ids = [str(uuid.uuid4()) for i in range(3)]


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
    m = MockEngine()
    m.endpoint_id = endpoint_uuid
    m.get_status_report.return_value = EPStatusReport(
        endpoint_id=endpoint_uuid, global_state={}, task_statuses=[]
    )

    yield m


@pytest.fixture
def mock_conf(mock_engine):
    yield UserEndpointConfig(engine=mock_engine)


@pytest.fixture
def mock_rp():
    pub_f = Future()
    pub_f.set_result(None)
    m = mock.Mock(spec=ResultPublisher)
    m.publish.return_value = pub_f
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
def mock_spt():
    with mock.patch(f"{_MOCK_BASE}setproctitle.setproctitle") as m:
        yield m


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


def test_detects_bad_executor_when_no_tasks(
    mock_log, ep_ix, mock_engine, randomstring, mock_rp, mock_tqs
):
    exc_text = randomstring()
    mock_engine.executor_exception = Exception(exc_text)

    ep_ix._main_loop()

    assert ep_ix.time_to_quit, "Sanity check"
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


def test_die_with_parent_goes_away_if_parent_dies(
    mocker, ep_ix_factory, mock_rp, mock_tqs
):
    ppid = random.randint(2, 1_000_000)

    mock_ppid = mocker.patch(f"{_MOCK_BASE}os.getppid")
    mock_ppid.side_effect = [ppid] * 100 + [1]  # sometime in future, parent dies
    ei = ep_ix_factory(parent_pid=ppid)
    mock_warn = mocker.patch.object(log, "warning")
    assert not ei.time_to_quit, "Verify test setup"

    ei.start()
    assert ei.time_to_quit

    warn_msg = str(list(a[0] for a, _ in mock_warn.call_args_list))
    assert "refusing to start" not in warn_msg
    assert f"Parent ({ppid}) has gone away" in warn_msg


def test_no_idle_if_not_configured(
    ep_ix_factory,
    mock_conf,
    mock_log,
    endpoint_uuid,
    mock_spt,
    mock_rp,
    mock_tqs,
    mock_pack,
):
    mock_conf.idle_heartbeats_soft = 0
    mock_conf.heartbeat_period = 1
    ei = ep_ix_factory(config=mock_conf)

    t = threading.Thread(target=ei._main_loop, daemon=True)
    t.start()

    try_assert(lambda: mock_log.debug.call_count > 500)
    ei.stop()
    t.join()
    assert not mock_spt.called


@pytest.mark.parametrize("idle_limit", range(1, 11))
def test_soft_idle_honored(
    mock_log,
    mock_conf,
    ep_ix_factory,
    mock_spt,
    idle_limit,
    mock_rp,
    mock_tqs,
    mock_pack,
):
    mock_conf.idle_heartbeats_soft = idle_limit
    ei = ep_ix_factory(config=mock_conf)

    task = Task(task_id=uuid.uuid4(), task_buffer=b"some test data")
    pheaders = {"task_uuid": task.task_id, "function_uuid": uuid.uuid4()}
    ei.pending_task_queue.put((1, pheaders, pack(task)))

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


@pytest.mark.parametrize("idle_limit", range(2, 12))
def test_hard_idle_honored(
    mocker,
    mock_log,
    mock_conf,
    ep_ix_factory,
    mock_spt,
    idle_limit,
    mock_rp,
    mock_tqs,
    mock_pack,
):
    idle_soft_limit = random.randrange(1, idle_limit)

    mocker.patch(f"{_MOCK_BASE}threading.Thread")

    mock_conf.idle_heartbeats_soft = idle_soft_limit
    mock_conf.idle_heartbeats_hard = idle_limit
    ei = ep_ix_factory(config=mock_conf)

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


def test_shutdown_lost_task_race_condition_sc36175(
    ep_ix_factory,
    mock_conf,
    mock_rp,
    mock_tqs,
):

    task = ("sometag", {"some": "headers"}, b"some bytes")

    def _get(*a, **k):
        ei.stop()
        return task

    ei = ep_ix_factory(config=mock_conf)
    ei.pending_task_queue = mock.Mock(spec=queue.SimpleQueue, get=_get)
    ei._main_loop()

    assert not mock_tqs.ack.called, "Expect task received at shutdown not acked"


def test_unidle_updates_proc_title(
    mock_log,
    mock_conf,
    ep_ix_factory,
    mock_spt,
    mock_quiesce,
    mock_rp,
    mock_tqs,
    mock_pack,
):
    orig_wait = mock_quiesce.wait.side_effect

    def block_until(*a, **k):
        main_thread_may_continue.wait()
        os.sched_yield()
        mock_quiesce.wait.side_effect = orig_wait

    def _submit(task_f, *a, **k):
        task_f.set_result(b"abcd")  # no_results > no_tasks, so soft idle is in play
        main_thread_may_continue.set()

    def insert_msg(*a, **k):
        # hard idle warning just happened; now go unidle by injecting a task;
        # soft idle will now be the exit path.
        task = Task(task_id=uuid.uuid4(), task_buffer=b"some test data")
        pheaders = {"task_uuid": task.task_id, "function_uuid": uuid.uuid4()}
        ei.pending_task_queue.put((1, pheaders, pack(task)))
        mock_quiesce.wait.side_effect = block_until
        mock_spt.side_effect = None  # back to normal mock behavior

    main_thread_may_continue = threading.Event()
    mock_conf.idle_heartbeats_soft = 1
    mock_conf.idle_heartbeats_hard = 3
    ei = ep_ix_factory(config=mock_conf)
    ei.engine.submit = mock.Mock(side_effect=_submit)
    mock_spt.side_effect = insert_msg

    ei._main_loop()

    msg = next(m[0][0] for m in mock_log.info.call_args_list if "Moved to" in m[0][0])
    assert msg.startswith("Moved to active state"), "expect why state changed"
    assert "due to " in msg

    first_idle, moved, *_, last_idle = (ca[0][0] for ca in mock_spt.call_args_list)
    assert first_idle.startswith("[possibly idle; shut down in ")
    assert "idle; " not in moved, "expected proc title reset when not idle"
    assert last_idle.startswith("[idle; shut down in ")


def test_sends_final_status_message_on_shutdown(
    mock_log,
    mock_conf,
    ep_ix_factory,
    endpoint_uuid,
    mock_rp,
    mock_tqs,
):
    mock_conf.idle_heartbeats_soft = 1
    mock_conf.idle_heartbeats_hard = 2
    ei = ep_ix_factory(config=mock_conf)
    ei._main_loop()

    packed_bytes = mock_rp.publish.call_args[0][0]
    epsr = unpack(packed_bytes)
    assert isinstance(epsr, EPStatusReport)
    assert epsr.endpoint_id == uuid.UUID(endpoint_uuid)
    assert epsr.global_state["active"] is False


def test_gracefully_handles_final_status_message_timeout(
    mock_log,
    mock_conf,
    ep_ix_factory,
    endpoint_uuid,
    mock_rp,
    mock_tqs,
    mock_pack,
):
    mock_conf.idle_heartbeats_soft = 1
    mock_conf.idle_heartbeats_hard = 2
    ei = ep_ix_factory(config=mock_conf)

    mock_future = mock.Mock(spec=Future)
    mock_future.result.side_effect = TimeoutError("asdfasdf")
    mock_rp.publish.return_value = mock_future
    ei._main_loop()

    assert mock_rp.publish.called


def test_sends_status_reports(
    mocker, ep_ix, endpoint_uuid, randomstring, mock_rp, mock_tqs
):
    status_reports = [
        EPStatusReport(endpoint_id=uuid.uuid4(), global_state={}, task_statuses=[])
        for i in range(5)
    ]
    ep_ix.engine.get_status_report.side_effect = status_reports
    ep_ix.config.idle_heartbeats_soft = 1
    ep_ix.config.idle_heartbeats_hard = 0  # will be set to soft + 1
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

    ep_ix._main_loop()
    assert not res_f.exists()


@pytest.mark.parametrize("allowed_fns", (None, _test_func_ids[:2], _test_func_ids[-1:]))
def test_epi_rejects_allowlist_task(
    endpoint_uuid, ep_ix, allowed_fns, randomstring, mock_rp, mock_tqs
):
    ep_ix.config.allowed_functions = allowed_fns

    func_to_run = _test_func_ids[-1]
    task_uuid = uuid.uuid4()
    task_msg = Task(task_id=task_uuid, task_buffer=randomstring())
    headers = {"function_uuid": str(func_to_run), "task_uuid": str(task_uuid)}

    ep_ix.pending_task_queue.put((1, headers, pack(task_msg)))

    t = threading.Thread(target=ep_ix._main_loop, daemon=True)
    t.start()
    try_assert(lambda: mock_rp.publish.called)
    ep_ix.stop()
    t.join()  # important to make sure we don't have a lockup

    msg_b = next(m for (m,), _ in mock_rp.publish.call_args_list if b'"result"' in m)
    res = unpack(msg_b)
    if allowed_fns is None or func_to_run in allowed_fns:
        assert res.data == task_msg.task_buffer, msg_b
    else:
        assert f"Function {func_to_run} not permitted" in res.data, ep_ix.config
        assert f"on endpoint {endpoint_uuid}" in res.data, msg_b
        assert res.details["fid"] == func_to_run
        assert res.details["eid"] == endpoint_uuid


def test_engine_submit_failure_reported(
    ep_ix, randomstring, mock_log, mock_tqs, mock_rp
):
    exc_text = randomstring()

    def infra_failure(*a, **k):
        raise Exception(exc_text)

    func_to_run = _test_func_ids[-1]
    task_uuid = uuid.uuid4()
    task_msg = Task(task_id=task_uuid, task_buffer=randomstring())
    headers = {"function_uuid": str(func_to_run), "task_uuid": str(task_uuid)}

    ep_ix.engine.submit = infra_failure
    ep_ix.pending_task_queue.put((1, headers, pack(task_msg)))

    t = threading.Thread(target=ep_ix._main_loop, daemon=True)
    t.start()
    try_assert(lambda: mock_rp.publish.called)
    ep_ix.stop()
    t.join()

    msg_b = next(m for (m,), _ in mock_rp.publish.call_args_list if b'"result"' in m)
    (lfmt,), _ = mock_log.exception.call_args

    assert exc_text.encode() in msg_b, "Expect engine failure published"
    assert f"Failed to process task {task_uuid}" == lfmt, "Expect logs see failure"


def test_tasks_audited_happy_path(
    ep_ix_factory, mock_conf, mock_log, mock_tqs, mock_rp, task_uuid, ez_pack_task
):
    r, w = os.pipe2(os.O_DIRECT)

    mock_conf.high_assurance = True
    ei = ep_ix_factory(config=mock_conf, audit_fd=w)
    assert not ei.time_to_quit

    f = engines.GCFuture(task_uuid)
    ei.audit(TaskState.RECEIVED, f)
    assert not ei.time_to_quit, "Canary test"
    msg = os.read(r, 4096).decode()
    assert TaskState.RECEIVED.name in msg, "Verify machinery hooked up"
    assert f"tid={str(task_uuid)}" in msg, "Verify machinery hooked up"

    task_bytes = ez_pack_task(double, 1)
    headers = {"function_uuid": str(task_uuid), "task_uuid": str(task_uuid)}
    ei.pending_task_queue.put((1, headers, task_bytes))

    t = threading.Thread(target=ei._main_loop, daemon=True)
    t.start()
    try_assert(lambda: mock_rp.publish.called)
    ei.stop()
    t.join()

    os.close(w)
    audit_msgs = ""
    while msg := os.read(r, 4096):
        audit_msgs += msg.decode() + "\n"
    os.close(r)
    assert TaskState.RECEIVED.name in audit_msgs
    assert TaskState.EXEC_START.name in audit_msgs
    assert TaskState.RUNNING.name in audit_msgs
    assert TaskState.EXEC_END.name in audit_msgs
    assert audit_msgs.count(f"fid={task_uuid}") == 4, "Expect func id with each record"
    assert audit_msgs.count(f"tid={task_uuid}") == 4, "Expect task id with each record"
    assert audit_msgs.count("bid=") == 4, "Expect known block state always shared"
    assert audit_msgs.count("bid=123") == 2, "Expect block shared when available"
    assert audit_msgs.count("jid=") == 2, "Expect job only shared if specified"


def test_tasks_audited_failed(
    ep_ix_factory, mock_conf, mock_log, mock_tqs, mock_rp, task_uuid, ez_pack_task
):
    r, w = os.pipe2(os.O_DIRECT)

    mock_conf.high_assurance = True
    mock_conf.allowed_functions = []
    ei = ep_ix_factory(config=mock_conf, audit_fd=w)

    task_bytes = ez_pack_task(double, 1)
    headers = {"function_uuid": str(task_uuid), "task_uuid": str(task_uuid)}
    ei.pending_task_queue.put((1, headers, task_bytes))

    t = threading.Thread(target=ei._main_loop, daemon=True)
    t.start()
    try_assert(lambda: mock_rp.publish.called)
    ei.stop()
    t.join()

    os.close(w)
    audit_msgs = ""
    while msg := os.read(r, 4096):
        audit_msgs += msg.decode() + "\n"
    os.close(r)
    assert TaskState.RECEIVED.name in audit_msgs
    assert TaskState.FAILED.name in audit_msgs
    assert TaskState.EXEC_START.name not in audit_msgs
