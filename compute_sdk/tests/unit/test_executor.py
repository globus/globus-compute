from __future__ import annotations

import random
import threading
import typing as t
import uuid
from unittest import mock

import pika
import pytest
from funcx_common import messagepack
from funcx_common.messagepack.message_types import Result, ResultErrorDetails
from tests.utils import try_assert, try_for_timeout

from funcx import FuncXClient, FuncXExecutor
from funcx.errors import FuncxTaskExecutionFailed
from funcx.sdk.asynchronous.funcx_future import FuncXFuture
from funcx.sdk.executor import TaskSubmissionInfo, _ResultWatcher
from funcx.serialize.facade import FuncXSerializer


def _is_stopped(thread: threading.Thread | None) -> bool:
    def _wrapped():
        return not (thread and thread.is_alive())

    return _wrapped


def noop():
    return 1


class MockedFuncXExecutor(FuncXExecutor):
    def __init__(self, *args, **kwargs):
        kwargs.update({"funcx_client": mock.Mock(spec=FuncXClient)})
        super().__init__(*args, **kwargs)
        self._time_to_stop_mock = threading.Event()
        self._task_submitter_exception: t.Type[Exception] | None = None

    def _task_submitter_impl(self):
        try:
            super()._task_submitter_impl()
        except Exception as exc:
            self._task_submitter_exception = exc


class MockedResultWatcher(_ResultWatcher):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._time_to_stop_mock = threading.Event()

    def start(self) -> None:
        super().start()
        try_assert(lambda: self._connection is not None)

    def run(self):
        self._connection = mock.MagicMock()
        self._channel = mock.MagicMock()
        self._time_to_stop_mock.wait()

    def shutdown(self, *args, **kwargs):
        super().shutdown(*args, **kwargs)
        self._time_to_stop_mock.set()

    def join(self, timeout: float | None = None) -> None:
        if self._time_to_stop:  # important to identify bugs
            self._time_to_stop_mock.set()
        super().join(timeout=timeout)


@pytest.fixture
def fxexecutor(mocker):
    fxc = mock.MagicMock()
    fxc.session_task_group_id = str(uuid.uuid4())
    fxe = FuncXExecutor(funcx_client=fxc)
    watcher = mocker.patch("funcx.sdk.executor._ResultWatcher", autospec=True)

    def create_mock_watcher(*args, **kwargs):
        return MockedResultWatcher(fxe)

    watcher.side_effect = create_mock_watcher

    yield fxc, fxe

    fxe.shutdown(wait=False, cancel_futures=True)
    try_for_timeout(_is_stopped(fxe._task_submitter))
    try_for_timeout(_is_stopped(fxe._result_watcher))

    if not _is_stopped(fxe._task_submitter)():
        trepr = repr(fxe._task_submitter)
        raise RuntimeError(
            "FuncXExecutor still running: _task_submitter thread alive: %s" % trepr
        )
    if not _is_stopped(fxe._result_watcher)():
        trepr = repr(fxe._result_watcher)
        raise RuntimeError(
            "FuncXExecutor still running: _result_watcher thread alive: %r" % trepr
        )


def test_task_submission_info_stringification():
    fut_id = 10
    func_id = "foo_func"
    ep_id = "bar_ep"

    info = TaskSubmissionInfo(
        task_num=fut_id, function_id=func_id, endpoint_id=ep_id, args=(), kwargs={}
    )
    as_str = str(info)
    assert as_str.startswith("TaskSubmissionInfo(")
    assert as_str.endswith("args=..., kwargs=...)")
    assert "task_num=10" in as_str
    assert "function_id='foo_func'" in as_str
    assert "endpoint_id='bar_ep'" in as_str


@pytest.mark.parametrize("argname", ("batch_interval", "batch_enabled"))
def test_deprecated_args_warned(argname, mocker):
    mock_warn = mocker.patch("funcx.sdk.executor.warnings")
    fxc = mock.Mock(spec=FuncXClient)
    FuncXExecutor(funcx_client=fxc).shutdown()
    mock_warn.warn.assert_not_called()

    FuncXExecutor(funcx_client=fxc, **{argname: 1}).shutdown()
    mock_warn.warn.assert_called()


def test_invalid_args_raise(randomstring):
    invalid_arg = f"abc_{randomstring()}"
    with pytest.raises(TypeError) as wrapped_err:
        FuncXExecutor(**{invalid_arg: 1}).shutdown()

    err = wrapped_err.value
    assert "invalid argument" in str(err)
    assert f"'{invalid_arg}'" in str(err)


def test_creates_default_client_if_none_provided(mocker):
    mock_fxc_klass = mocker.patch("funcx.sdk.executor.FuncXClient")
    FuncXExecutor().shutdown()

    mock_fxc_klass.assert_called()


def test_executor_shutdown(fxexecutor):
    fxc, fxe = fxexecutor
    fxe.shutdown()

    try_assert(_is_stopped(fxe._task_submitter))
    try_assert(_is_stopped(fxe._result_watcher))


def test_executor_context_manager(fxexecutor):
    fxc, fxe = fxexecutor
    with fxe:
        pass
    assert _is_stopped(fxe._task_submitter)
    assert _is_stopped(fxe._result_watcher)


def test_property_task_group_id_is_isolated(fxexecutor):
    fxc, fxe = fxexecutor
    assert fxe.task_group_id != fxc.session_task_group_id

    fxe.task_group_id = uuid.uuid4()
    assert fxe.task_group_id != fxc.session_task_group_id


def test_multiple_register_function_fails(fxexecutor):
    fxc, fxe = fxexecutor

    fxc.register_function.return_value = "abc"

    fxe.register_function(noop)

    with pytest.raises(ValueError):
        fxe.register_function(noop)

    try_assert(lambda: fxe._stopped)

    with pytest.raises(RuntimeError):
        fxe.register_function(noop)


def test_shortcut_register_function(fxexecutor):
    fxc, fxe = fxexecutor

    fn_id = str(uuid.uuid4())
    fxe.register_function(noop, function_id=fn_id)

    with pytest.raises(ValueError):
        fxe.register_function(noop, function_id=fn_id)

    fxc.register_function.assert_not_called()


def test_failed_registration_shuts_down_executor(fxexecutor, randomstring):
    fxc, fxe = fxexecutor

    exc_text = randomstring()
    fxc.register_function.side_effect = Exception(exc_text)

    with pytest.raises(Exception) as wrapped_exc:
        fxe.register_function(noop)

    exc = wrapped_exc.value
    assert exc_text in str(exc)
    try_assert(lambda: fxe._stopped)


def test_submit_raises_if_thread_stopped(fxexecutor):
    fxc, fxe = fxexecutor
    fxe.shutdown()

    try_assert(_is_stopped(fxe._task_submitter), "Test prerequisite")

    with pytest.raises(RuntimeError) as wrapped_exc:
        fxe.submit(noop)

    err = wrapped_exc.value
    assert " is shutdown;" in str(err)


def test_submit_auto_registers_function(fxexecutor):
    fxc, fxe = fxexecutor

    fxc.register_function.return_value = "abc"
    fxe.endpoint_id = "some_ep_id"
    fxe.submit(noop)

    assert fxc.register_function.called


def test_submit_value_error_if_no_endpoint(fxexecutor):
    fxc, fxe = fxexecutor

    with pytest.raises(ValueError) as pytest_exc:
        fxe.submit(noop)

    err = pytest_exc.value
    assert "No endpoint_id set" in str(err)
    assert "    fxe = FuncXExecutor(endpoint_id=" in str(err), "Expected hint"
    try_assert(_is_stopped(fxe._task_submitter), "Expected graceful shutdown on error")


def test_same_function_different_containers_allowed(fxexecutor):
    fxc, fxe = fxexecutor
    c1_id, c2_id = str(uuid.uuid4()), str(uuid.uuid4())

    fxe.container_id = c1_id
    fxe.register_function(noop)
    fxe.container_id = c2_id
    fxe.register_function(noop)
    with pytest.raises(ValueError, match="already registered"):
        fxe.register_function(noop)


def test_map_raises(fxexecutor):
    fxc, fxe = fxexecutor

    with pytest.raises(NotImplementedError):
        fxe.map(noop)


@pytest.mark.parametrize("num_tasks", [0, 1, 2, 10])
def test_reload_tasks_none_completed(fxexecutor, mocker, num_tasks):
    fxc, fxe = fxexecutor

    mock_log = mocker.patch("funcx.sdk.executor.log")

    mock_data = {
        "taskgroup_id": fxe.task_group_id,
        "tasks": [{"id": uuid.uuid4()} for _ in range(num_tasks)],
    }
    mock_batch_result = {t["id"]: t for t in mock_data["tasks"]}
    mock_batch_result = mock.MagicMock(data={"results": mock_batch_result})

    fxc.web_client.get_taskgroup_tasks.return_value = mock_data
    fxc.web_client.get_batch_status.return_value = mock_batch_result

    client_futures = list(fxe.reload_tasks())
    if num_tasks == 0:
        log_args, log_kwargs = mock_log.warning.call_args
        assert "Received no tasks" in log_args[0]
        assert fxe.task_group_id in log_args[0]
    else:
        assert not mock_log.warning.called

    assert len(client_futures) == num_tasks
    assert not any(fut.done() for fut in client_futures)


@pytest.mark.parametrize("num_tasks", [1, 2, 10])
def test_reload_tasks_some_completed(fxexecutor, mocker, num_tasks):
    fxc, fxe = fxexecutor

    mock_log = mocker.patch("funcx.sdk.executor.log")

    mock_data = {
        "taskgroup_id": fxe.task_group_id,
        "tasks": [{"id": uuid.uuid4()} for _ in range(num_tasks)],
    }
    num_completed = random.randint(1, num_tasks)
    num_i = 0

    serialize = FuncXSerializer().serialize
    mock_batch_result = {t["id"]: t for t in mock_data["tasks"]}
    for t_id in mock_batch_result:
        if num_i >= num_completed:
            break
        num_i += 1
        mock_batch_result[t_id]["completion_t"] = "0"
        mock_batch_result[t_id]["status"] = "success"
        mock_batch_result[t_id]["result"] = serialize("abc")
    mock_batch_result = mock.MagicMock(data={"results": mock_batch_result})

    fxc.web_client.get_taskgroup_tasks.return_value = mock_data
    fxc.web_client.get_batch_status.return_value = mock_batch_result

    client_futures = list(fxe.reload_tasks())
    if num_tasks == 0:
        log_args, log_kwargs = mock_log.warning.call_args
        assert "Received no tasks" in log_args[0]
        assert fxe.task_group_id in log_args[0]
    else:
        assert not mock_log.warning.called

    assert len(client_futures) == num_tasks
    assert sum(1 for fut in client_futures if fut.done()) == num_completed


def test_reload_tasks_all_completed(fxexecutor):
    fxc, fxe = fxexecutor

    serialize = FuncXSerializer().serialize
    num_tasks = 5

    mock_data = {
        "taskgroup_id": fxe.task_group_id,
        "tasks": [
            {
                "id": uuid.uuid4(),
                "completion_t": 25,
                "status": "success",
                "result": serialize("abc"),
            }
            for _ in range(num_tasks)
        ],
    }

    mock_batch_result = {t["id"]: t for t in mock_data["tasks"]}
    mock_batch_result = mock.MagicMock(data={"results": mock_batch_result})

    fxc.web_client.get_taskgroup_tasks.return_value = mock_data
    fxc.web_client.get_batch_status.return_value = mock_batch_result

    client_futures = list(fxe.reload_tasks())

    assert len(client_futures) == num_tasks
    assert sum(1 for fut in client_futures if fut.done()) == num_tasks
    assert fxe._result_watcher is None, "Should NOT start watcher: all tasks done!"


def test_reload_starts_new_watcher(fxexecutor):
    fxc, fxe = fxexecutor

    num_tasks = 3

    mock_data = {
        "taskgroup_id": fxe.task_group_id,
        "tasks": [{"id": uuid.uuid4()} for _ in range(num_tasks)],
    }
    mock_batch_result = {t["id"]: t for t in mock_data["tasks"]}
    mock_batch_result = mock.MagicMock(data={"results": mock_batch_result})

    fxc.web_client.get_taskgroup_tasks.return_value = mock_data
    fxc.web_client.get_batch_status.return_value = mock_batch_result

    client_futures = list(fxe.reload_tasks())

    assert len(client_futures) == num_tasks
    try_assert(lambda: fxe._result_watcher.is_alive())
    watcher_1 = fxe._result_watcher

    client_futures = list(fxe.reload_tasks())
    try_assert(lambda: fxe._result_watcher.is_alive())
    watcher_2 = fxe._result_watcher

    assert watcher_1 is not watcher_2


def test_reload_tasks_cancels_existing_futures(fxexecutor, randomstring):
    fxc, fxe = fxexecutor

    def mock_data():
        return {
            "taskgroup_id": fxe.task_group_id,
            "tasks": [{"id": uuid.uuid4()} for i in range(random.randint(0, 20))],
        }

    fxc.web_client.get_taskgroup_tasks.return_value = mock_data()

    client_futures_1 = list(fxe.reload_tasks())
    fxc.get_taskgroup_tasks.return_value = mock_data()
    client_futures_2 = list(fxe.reload_tasks())

    assert all(fut.done() for fut in client_futures_1)
    assert all(fut.cancelled() for fut in client_futures_1)
    assert not any(fut.done() for fut in client_futures_2)


def test_reload_client_taskgroup_tasks_fails_gracefully(fxexecutor):
    fxc, fxe = fxexecutor

    mock_datum = (
        (KeyError, {"mispeleed": fxe.task_group_id}),
        (ValueError, {"taskgroup_id": "abcd"}),
        (None, {"taskgroup_id": fxe.task_group_id}),
    )

    for expected_exc_class, md in mock_datum:
        fxc.web_client.get_taskgroup_tasks.return_value = md
        if expected_exc_class:
            with pytest.raises(expected_exc_class):
                fxe.reload_tasks()
        else:
            fxe.reload_tasks()


def test_reload_sets_failed_tasks(fxexecutor):
    fxc, fxe = fxexecutor

    mock_data = {
        "taskgroup_id": fxe.task_group_id,
        "tasks": [
            {"id": uuid.uuid4(), "completion_t": 1, "exception": "doh!"}
            for i in range(random.randint(0, 10))
        ],
    }

    mock_batch_result = {t["id"]: t for t in mock_data["tasks"]}
    mock_batch_result = mock.MagicMock(data={"results": mock_batch_result})

    fxc.web_client.get_taskgroup_tasks.return_value = mock_data
    fxc.web_client.get_batch_status.return_value = mock_batch_result

    futs = list(fxe.reload_tasks())

    assert all(fut.done() for fut in futs)
    assert all("doh!" in str(fut.exception()) for fut in futs)


def test_reload_handles_deseralization_error_gracefully(fxexecutor):
    fxc, fxe = fxexecutor
    fxc.fx_serializer = FuncXSerializer()

    mock_data = {
        "taskgroup_id": fxe.task_group_id,
        "tasks": [
            {"id": uuid.uuid4(), "completion_t": 1, "result": "a", "status": "success"}
            for i in range(random.randint(0, 10))
        ],
    }

    mock_batch_result = {t["id"]: t for t in mock_data["tasks"]}
    mock_batch_result = mock.MagicMock(data={"results": mock_batch_result})

    fxc.web_client.get_taskgroup_tasks.return_value = mock_data
    fxc.web_client.get_batch_status.return_value = mock_batch_result

    futs = list(fxe.reload_tasks())

    assert all(fut.done() for fut in futs)
    assert all("Failed to set " in str(fut.exception()) for fut in futs)


@pytest.mark.parametrize("batch_size", tuple(range(1, 11)))
def test_task_submitter_respects_batch_size(fxexecutor, batch_size: int):
    fxc, fxe = fxexecutor

    fxc.create_batch.side_effect = mock.MagicMock
    fxc.register_function.return_value = "abc"
    num_batches = 50

    fxe.endpoint_id = "some_ep_id"
    fxe.batch_size = batch_size
    for _ in range(num_batches * batch_size):
        fxe.submit(noop)
    fxe.shutdown(cancel_futures=True)

    for args, _kwargs in fxc.batch_run.call_args_list:
        batch, *_ = args
        assert batch.add.call_count <= batch_size


def test_task_submitter_stops_executor_on_exception():
    fxe = MockedFuncXExecutor()
    fxe._tasks_to_send.put(("too", "much", "destructuring", "!!"))

    try_assert(lambda: fxe._stopped)
    try_assert(lambda: isinstance(fxe._task_submitter_exception, ValueError))


def test_task_submitter_stops_executor_on_upstream_error_response(randomstring):
    fxe = MockedFuncXExecutor()

    upstream_error = Exception(f"Upstream error {randomstring}!!")
    fxe.funcx_client.batch_run.side_effect = upstream_error
    fxe.task_group_id = "abc"
    tsi = TaskSubmissionInfo(
        task_num=12345, function_id="abc", endpoint_id="abc", args=(), kwargs={}
    )
    fxe._tasks_to_send.put((FuncXFuture(), tsi))

    try_assert(lambda: fxe._stopped)
    try_assert(lambda: str(upstream_error) == str(fxe._task_submitter_exception))


def test_task_submitter_handles_stale_result_watcher_gracefully(fxexecutor, mocker):
    fxc, fxe = fxexecutor
    fxe.endpoint_id = "blah"

    task_id = str(uuid.uuid4())
    fxc.batch_run.return_value = [task_id]
    fxe.submit(noop)
    try_assert(lambda: bool(fxe._result_watcher), "Test prerequisite")
    try_assert(lambda: bool(fxe._result_watcher._open_futures), "Test prerequisite")
    watcher_1 = fxe._result_watcher
    watcher_1._closed = True  # simulate shutting down, but not yet stopped
    watcher_1._time_to_stop = True

    fxe.submit(noop)
    try_assert(lambda: fxe._result_watcher is not watcher_1, "Test prerequisite")


def test_task_submitter_sets_future_task_ids(fxexecutor):
    fxc, fxe = fxexecutor

    num_tasks = random.randint(2, 20)
    futs = [FuncXFuture() for _ in range(num_tasks)]
    batch_ids = [uuid.uuid4() for _ in range(num_tasks)]

    fxc.batch_run.return_value = batch_ids
    fxe._submit_tasks(futs, [])

    assert all(f.task_id == task_id for f, task_id in zip(futs, batch_ids))


def test_resultwatcher_stops_if_unable_to_connect(mocker):
    mock_time = mocker.patch("funcx.sdk.executor.time")
    fxe = mock.Mock(spec=FuncXExecutor)
    rw = _ResultWatcher(fxe)
    rw._connect = mock.Mock(return_value=mock.Mock(spec=pika.SelectConnection))

    rw.run()
    assert rw._connection_tries >= rw.connect_attempt_limit
    assert mock_time.sleep.call_count == rw._connection_tries - 1, "Should wait between"


def test_resultwatcher_ignores_invalid_tasks(mocker):
    fxe = mock.Mock(spec=FuncXExecutor)
    rw = _ResultWatcher(fxe)
    rw._connect = mock.Mock(return_value=mock.Mock(spec=pika.SelectConnection))

    futs = [FuncXFuture() for i in range(random.randint(1, 10))]
    futs[0].task_id = uuid.uuid4()
    num_added = rw.watch_for_task_results(futs)
    assert 1 == num_added


def test_resultwatcher_cancels_futures_on_unexpected_stop(mocker):
    mocker.patch("funcx.sdk.executor.time")
    fxe = mock.Mock(spec=FuncXExecutor)
    rw = _ResultWatcher(fxe)
    rw._connect = mock.Mock(return_value=mock.Mock(spec=pika.SelectConnection))

    fut = FuncXFuture(task_id=uuid.uuid4())
    rw.watch_for_task_results([fut])
    rw.run()

    assert "thread quit" in str(fut.exception())


def test_resultwatcher_gracefully_handles_unexpected_exception(mocker):
    mocker.patch("funcx.sdk.executor.time")
    mock_log = mocker.patch("funcx.sdk.executor.log")
    fxe = mock.Mock(spec=FuncXExecutor)
    rw = _ResultWatcher(fxe)
    rw._connect = mock.Mock(return_value=mock.Mock(spec=pika.SelectConnection))
    rw._event_watcher = mock.Mock(side_effect=Exception)

    rw.run()

    assert mock_log.exception.call_count > 2
    args, _kwargs = mock_log.exception.call_args
    assert "shutting down" in args[0]


def test_resultwatcher_blocks_until_tasks_done():
    fut = FuncXFuture(task_id=uuid.uuid4())
    mrw = MockedResultWatcher(mock.Mock())
    mrw.watch_for_task_results([fut])
    mrw.start()

    res = Result(task_id=fut.task_id, data="abc123")
    mrw._received_results[fut.task_id] = (None, res)

    mrw.shutdown(wait=False)
    try_assert(lambda: not mrw._time_to_stop, timeout_ms=1000)
    mrw._match_results_to_futures()
    try_assert(lambda: mrw._time_to_stop)


def test_resultwatcher_does_not_check_if_no_results():
    fut = FuncXFuture(task_id=uuid.uuid4())
    mrw = MockedResultWatcher(mock.Mock())
    mrw._match_results_to_futures = mock.Mock()
    mrw.watch_for_task_results([fut])
    mrw.start()
    mrw._event_watcher()

    mrw._match_results_to_futures.assert_not_called()
    mrw.shutdown(cancel_futures=True)


def test_resultwatcher_checks_match_if_results():
    fut = FuncXFuture(task_id=uuid.uuid4())
    res = Result(task_id=fut.task_id, data="abc123")

    mrw = MockedResultWatcher(mock.Mock())
    mrw._received_results[fut.task_id] = (None, res)

    mrw.watch_for_task_results([fut])
    mrw.start()
    mrw._event_watcher()

    assert fut.done() and not fut.cancelled()
    assert fut.result() is not None
    mrw.shutdown(cancel_futures=True)


def test_resultwatcher_repr():
    mrw = MockedResultWatcher(mock.Mock())
    assert "<✗;" in repr(mrw)
    mrw._consumer_tag = "asdf"
    assert "<✓;" in repr(mrw)
    mrw._consumer_tag = None
    assert "<✗;" in repr(mrw)

    for i in range(10):
        assert f"fut={i};" in repr(mrw)
        mrw._open_futures[f"a{i}"] = 1
    mrw._open_futures.update({f"b{i}": 1 for i in range(1000)})
    assert "fut=1,010; " in repr(mrw), "includes separator"

    for i in range(10):
        assert f"res={i};" in repr(mrw)
        mrw._received_results[f"a{i}"] = 1
    mrw._received_results.update({f"b{i}": 1 for i in range(1000)})
    assert "res=1,010; " in repr(mrw), "includes separator"

    assert "; qp=-" in repr(mrw), "blank queue prefix shown with dash (-)"
    mrw._queue_prefix = "abc"
    assert f"; qp={mrw._queue_prefix}" in repr(mrw)


def test_resultwatcher_match_sets_exception(randomstring):
    payload = randomstring()
    fxs = FuncXSerializer()
    fut = FuncXFuture(task_id=uuid.uuid4())
    err_details = ResultErrorDetails(code="1234", user_message="some_user_message")
    res = Result(task_id=fut.task_id, error_details=err_details, data=payload)

    mrw = MockedResultWatcher(mock.Mock())
    mrw.funcx_executor.funcx_client.fx_serializer.deserialize = fxs.deserialize
    mrw._received_results[fut.task_id] = (mock.Mock(timestamp=5), res)
    mrw.watch_for_task_results([fut])
    mrw.start()
    mrw._event_watcher()

    assert payload in str(fut.exception())
    assert isinstance(fut.exception(), FuncxTaskExecutionFailed)
    mrw.shutdown()


def test_resultwatcher_match_sets_result(randomstring):
    payload = randomstring()
    fxs = FuncXSerializer()
    fut = FuncXFuture(task_id=uuid.uuid4())
    res = Result(task_id=fut.task_id, data=fxs.serialize(payload))

    mrw = MockedResultWatcher(mock.Mock())
    mrw.funcx_executor.funcx_client.fx_serializer.deserialize = fxs.deserialize
    mrw._received_results[fut.task_id] = (None, res)
    mrw.watch_for_task_results([fut])
    mrw.start()
    mrw._event_watcher()

    assert fut.result() == payload
    mrw.shutdown()


def test_resultwatcher_match_handles_deserialization_error():
    invalid_payload = "invalidly serialized"
    fxs = FuncXSerializer()
    fut = FuncXFuture(task_id=uuid.uuid4())
    res = Result(task_id=fut.task_id, data=invalid_payload)

    mrw = MockedResultWatcher(mock.Mock())
    mrw.funcx_executor.funcx_client.fx_serializer.deserialize = fxs.deserialize
    mrw._received_results[fut.task_id] = (None, res)
    mrw.watch_for_task_results([fut])
    mrw.start()
    mrw._event_watcher()

    exc = fut.exception()
    assert "Malformed or unexpected data structure" in str(exc)
    assert invalid_payload in str(exc)
    mrw.shutdown()


@pytest.mark.parametrize("unpacked", ("not_a_Result", Exception))
def test_resultwatcher_onmessage_verifies_result_type(mocker, unpacked):
    mock_unpack = mocker.patch("funcx.sdk.executor.messagepack.unpack")

    mock_unpack.side_effect = unpacked
    mock_channel = mock.Mock()
    mock_deliver = mock.Mock()
    mock_props = mock.Mock()
    mrw = MockedResultWatcher(mock.Mock())
    mrw._on_message(mock_channel, mock_deliver, mock_props, b"some_bytes")
    mock_channel.basic_nack.assert_called()
    assert not mrw._received_results


def test_resultwatcher_onmessage_sets_check_results_flag():
    res = Result(task_id=uuid.uuid4(), data="abc")

    mock_channel = mock.Mock()
    mock_deliver = mock.Mock()
    mock_props = mock.Mock()
    mrw = MockedResultWatcher(mock.Mock())
    mrw._on_message(mock_channel, mock_deliver, mock_props, messagepack.pack(res))
    mock_channel.basic_nack.assert_not_called()
    assert mrw._received_results
    assert mrw._time_to_check_results.is_set()


@pytest.mark.parametrize("exc", (MemoryError("some description"), "some description"))
def test_resultwatcher_stops_loop_on_open_failure(mocker, exc):
    mock_log = mocker.patch("funcx.sdk.executor.log", autospec=True)

    mrw = MockedResultWatcher(mock.Mock())
    mrw.start()
    assert not mrw._connection.ioloop.stop.called, "Test setup verification"

    while not mrw._cancellation_reason:
        mrw._connection_tries += 1

        assert not mrw._cancellation_reason, "Test setup verification"
        assert not mock_log.warning.called
        mrw._connection.ioloop.stop.reset_mock()
        mock_log.debug.reset_mock()

        mrw._on_open_failed(mock.Mock(), exc)  # kernel of test

        assert mrw._connection.ioloop.stop.called
        assert mock_log.debug.called
        log_args, *_ = mock_log.debug.call_args
        assert "Failed to open connection" in log_args[0]

    assert mrw._connection_tries == mrw.connect_attempt_limit
    assert mock_log.warning.called, "Expected warning only on watcher quit"
    assert "some description" in str(mrw._cancellation_reason)
    mrw.shutdown()


def test_resultwatcher_connection_closed_stops_loop():
    exc = MemoryError("some description")
    mrw = MockedResultWatcher(mock.Mock())
    mrw.start()
    mrw._connection.ioloop.stop.assert_not_called()
    mrw._on_connection_closed(mock.Mock(), exc)
    mrw._connection.ioloop.stop.assert_called()
    mrw.shutdown()


def test_resultwatcher_channel_closed_retries_then_shuts_down():
    exc = Exception("some pika reason")
    mrw = MockedResultWatcher(mock.Mock())
    mrw.start()
    for i in range(1, mrw.channel_close_window_limit):
        mrw._connection.ioloop.call_later.reset_mock()
        mrw._on_channel_closed(mock.Mock(), exc)
        assert len(mrw._channel_closes) == i
    assert not mrw._closed
    mrw._on_channel_closed(mock.Mock(), exc)
    assert mrw._closed

    # and finally, no error if we call "too many" times
    mrw._on_channel_closed(mock.Mock(), exc)


def test_resultwatcher_connection_opened_resets_fail_counter():
    mrw = MockedResultWatcher(mock.Mock())
    mrw.start()
    mrw._connection_tries = 57
    mrw._on_connection_open(None)
    assert mrw._connection_tries == 0
    mrw.shutdown()


def test_resultwatcher_channel_opened_starts_consuming():
    mock_channel = mock.Mock()
    mrw = MockedResultWatcher(mock.Mock())
    mrw.start()
    assert mrw._consumer_tag is None
    mrw._on_channel_open(mock_channel)
    assert mock_channel is mrw._channel
    assert mrw._consumer_tag is not None
    mrw.shutdown()


def test_resultwatcher_amqp_acks_in_bulk():
    mrw = MockedResultWatcher(mock.Mock())
    mrw.start()
    mrw._to_ack.extend(range(200))
    assert mrw._channel.basic_ack.call_count == 0
    mrw._event_watcher()
    assert not mrw._to_ack
    assert mrw._channel.basic_ack.call_count == 1
    mrw.shutdown()
