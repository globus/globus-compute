import logging
import pathlib
import random
import time
from concurrent.futures import Future
from unittest import mock

import pytest
from globus_compute_common import messagepack
from globus_compute_common.messagepack.message_types import (
    EPStatusReport,
    Result,
    TaskTransition,
)
from globus_compute_common.tasks import ActorName, TaskState
from globus_compute_endpoint.endpoint.config import UserEndpointConfig
from globus_compute_endpoint.endpoint.config.utils import serialize_config
from globus_compute_endpoint.engines import (
    GCFuture,
    GlobusComputeEngine,
    GlobusMPIEngine,
    ProcessPoolEngine,
    ThreadPoolEngine,
)
from globus_compute_endpoint.engines.base import GlobusComputeEngineBase
from globus_compute_sdk.sdk.utils.uuid_like import as_uuid
from globus_compute_sdk.serialize.concretes import SELECTABLE_STRATEGIES
from parsl import HighThroughputExecutor
from parsl.providers import KubernetesProvider
from tests.utils import kill_manager

logger = logging.getLogger(__name__)


class MockGCEngine(GlobusComputeEngineBase):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self._task_counter = random.randint(0, 1_000)
        self.test_fail_count = 0

    def assert_ha_compliant(self):
        pass

    def start(self, *a, endpoint_id, **k):
        self.endpoint_id = as_uuid(endpoint_id)
        self._engine_ready = True

    def get_status_report(self) -> EPStatusReport:
        return EPStatusReport(
            endpoint_id=self.endpoint_id, global_state={}, task_statuses={}
        )

    def _submit(self, *a, **k):
        self._task_counter += 1
        f = Future()
        f.executor_task_id = self._task_counter
        if self.test_fail_count < self.max_retries_on_system_failure:
            f.set_exception(Exception("Some infra exception"))
        else:
            f.set_result(None)
        return f

    def shutdown(self, /, **kwargs) -> None:
        self._engine_ready = False


@pytest.fixture
def gce():
    e = GlobusComputeEngine(address="::1")
    yield e
    e.shutdown()


def test_result_message_packing(serde, task_uuid):
    exec_start = TaskTransition(
        timestamp=time.time_ns(), state=TaskState.EXEC_START, actor=ActorName.WORKER
    )

    result = random.randint(0, 1000)

    exec_end = TaskTransition(
        timestamp=time.time_ns(), state=TaskState.EXEC_END, actor=ActorName.WORKER
    )
    result_message = dict(
        task_id=task_uuid,
        data=serde.serialize(result),
        task_statuses=[exec_start, exec_end],
    )

    mResult = Result(**result_message)
    assert isinstance(mResult, Result)
    packed_result = messagepack.pack(mResult)
    assert isinstance(packed_result, bytes)

    unpacked = messagepack.unpack(packed_result)
    assert isinstance(unpacked, Result)

    assert unpacked.task_id == task_uuid
    assert serde.deserialize(unpacked.data) == result


@pytest.mark.parametrize(
    "engine_type",
    (ProcessPoolEngine, ThreadPoolEngine, GlobusComputeEngine),
)
def test_allowed_serializers_passthrough_to_serde(engine_type, engine_runner) -> None:
    engine: GlobusComputeEngineBase
    if hasattr(engine_type, "_ExecutorClass"):
        with mock.patch.object(engine_type, "_ExecutorClass") as mock_ex:
            mock_ex.__name__ = "ClassName"
            mock_ex.return_value = mock.Mock(launch_cmd="")
            engine = engine_type(allowed_serializers=SELECTABLE_STRATEGIES)
    else:
        engine = engine_type(allowed_serializers=SELECTABLE_STRATEGIES)

    assert engine.serde is not None
    assert engine.serde.allowed_deserializer_types == set(SELECTABLE_STRATEGIES)


@pytest.mark.parametrize("max_fails", (0, 1, 2, 3, 4))
def test_executor_id_bookkeeping(task_uuid, max_fails):
    engine = MockGCEngine(max_retries_on_system_failure=max_fails)
    engine.start(endpoint_id=task_uuid)
    task_f = GCFuture(gc_task_id=task_uuid)
    engine.submit(task_f, b"SomeBytes", {})
    task_f.result()
    assert task_f.executor_task_id == engine._task_counter


@pytest.mark.parametrize(
    "EngineClass",
    (ThreadPoolEngine, ProcessPoolEngine, GlobusComputeEngine, GlobusMPIEngine),
)
def test_engines_executor_id(ez_pack_task, task_uuid, EngineClass):
    if EngineClass in (GlobusComputeEngine, GlobusMPIEngine):
        with mock.patch.object(EngineClass, "_ExecutorClass") as mock_ex:
            mock_ex.__name__ = "ClassName"
            mock_ex.return_value = mock.Mock(launch_cmd="")
            engine = EngineClass(address="::1")
            engine._engine_ready = True
    else:
        engine = EngineClass(max_workers=1)
        engine.executor = mock.Mock(spec=EngineClass)
        engine._engine_ready = True

    ex_task_id = random.randint(1, 1_1000)

    def mock_ex_submit(*a, **k):
        f = Future()
        f.set_result(None)
        f.parsl_executor_task_id = ex_task_id
        f.executor_task_id = ex_task_id
        return f

    engine.executor.submit = mock_ex_submit

    f = GCFuture(gc_task_id=task_uuid)
    engine.submit(f, b"task bytes", {})
    f.result()
    assert f.executor_task_id is not None


def test_gc_engine_system_failure(serde, ez_pack_task, task_uuid, engine_runner):
    """Test behavior of engine failure killing task"""
    engine = engine_runner(GlobusComputeEngine, max_retries_on_system_failure=0)

    task_bytes = ez_pack_task(kill_manager)
    task_f = GCFuture(gc_task_id=task_uuid)
    engine.submit(task_f, task_bytes, {})
    r = messagepack.unpack(task_f.result())
    assert isinstance(r, Result)
    assert r.task_id == task_uuid
    assert r.is_error
    assert r.error_details.code == "RemoteExecutionError"
    assert "ManagerLost" in r.data


def test_serialized_engine_config_has_provider():
    loopback = "::1"
    ep_config = UserEndpointConfig(executors=[GlobusComputeEngine(address=loopback)])

    res = serialize_config(ep_config)
    assert res["engine"]["executor"].get("provider")
    ep_config.engine.shutdown()


def test_gcengine_compute_launch_cmd(gce):
    assert gce.executor.launch_cmd.startswith(
        "globus-compute-endpoint python-exec"
        " parsl.executors.high_throughput.process_worker_pool"
    )
    assert "process_worker_pool.py" not in gce.executor.launch_cmd


def test_gcengine_compute_interchange_launch_cmd(gce):
    assert gce.executor.interchange_launch_cmd[:3] == [
        "globus-compute-endpoint",
        "python-exec",
        "parsl.executors.high_throughput.interchange",
    ]
    assert "interchange.py" not in gce.executor.interchange_launch_cmd


def test_gcengine_pass_through_to_executor(randomstring):
    args = ("arg1", 2)
    kwargs = {
        "label": "VroomEngine",
        "address": "::1",
        "encrypted": False,
        "max_workers_per_node": 1,
        "foo": "bar",
        randomstring(): randomstring(),
    }

    m = mock.Mock(launch_cmd="")
    with mock.patch.object(GlobusComputeEngine, "_ExecutorClass") as mock_ex:
        mock_ex.__name__ = randomstring()
        mock_ex.return_value = m
        engine = GlobusComputeEngine(*args, **kwargs)
        kwargs["label"] = f"{kwargs['label']}-{mock_ex.__name__}"
        engine.shutdown()
    a, k = mock_ex.call_args
    assert a == args
    assert kwargs == k


def test_gcengine_start_pass_through_to_executor(tmp_path: pathlib.Path, endpoint_uuid):
    mock_ex = mock.Mock(
        status_polling_interval=5,
        launch_cmd="foo-bar",
        interchange_launch_cmd="foo-bar",
    )

    scripts_dir = str(tmp_path / "submit_scripts")

    with mock.patch.object(GlobusComputeEngine, "_ExecutorClass", mock.Mock):
        engine = GlobusComputeEngine(executor=mock_ex)

    assert mock_ex.run_dir != tmp_path
    assert mock_ex.provider.script_dir != scripts_dir

    engine.start(endpoint_id=endpoint_uuid, run_dir=tmp_path)
    engine.shutdown()

    assert mock_ex.run_dir == tmp_path
    assert mock_ex.provider.script_dir == scripts_dir


def test_gcengine_start_provider_without_channel(tmp_path: pathlib.Path, endpoint_uuid):
    mock_executor = mock.Mock(spec=HighThroughputExecutor)
    mock_executor.status_polling_interval = 5
    mock_executor.provider = mock.Mock(spec=KubernetesProvider)
    mock_executor.launch_cmd = "foo-bar"
    mock_executor.interchange_launch_cmd = "foo-bar"

    assert not hasattr(mock_executor.provider, "channel"), "Verify test setup"

    engine = GlobusComputeEngine(executor=mock_executor)
    engine.start(endpoint_id=endpoint_uuid, run_dir=tmp_path)
    engine.shutdown()


@pytest.mark.parametrize("encrypted", (True, False))
def test_gcengine_encrypted(encrypted: bool, engine_runner):
    engine: GlobusComputeEngine = engine_runner(
        GlobusComputeEngine, encrypted=encrypted
    )
    assert engine.encrypted is engine.executor.encrypted is encrypted

    with pytest.raises(AttributeError):
        engine.encrypted = not encrypted

    engine.executor.encrypted = not encrypted
    assert engine.encrypted is not encrypted


def test_gcengine_new_executor_not_exceptional(gce):
    assert gce.executor_exception is None, "Expect no exception from fresh Executor"


def test_gcengine_executor_exception_passthrough(randomstring, gce):
    exc_text = randomstring()
    gce.executor.set_bad_state_and_fail_all(ZeroDivisionError(exc_text))
    assert isinstance(gce.executor_exception, ZeroDivisionError)
    assert exc_text in str(gce.executor_exception)


def test_gcengine_bad_state_futures_failed_immediately(randomstring, task_uuid, gce):
    gce._engine_ready = True
    exc_text = randomstring()
    gce.executor.set_bad_state_and_fail_all(ZeroDivisionError(exc_text))

    taskb = b"some packed task bytes"
    futs = [GCFuture(gc_task_id=task_uuid) for _ in range(5)]
    for task_f in futs:
        gce.submit(task_f=task_f, packed_task=taskb, resource_specification={})

    assert all(f.done() for f in futs), "Expect immediate completion for failed state"
    assert all(exc_text in str(f.result()) for f in futs)


def test_gcengine_exception_report_from_bad_state(task_uuid, gce):
    gce._engine_ready = True
    gce.executor.set_bad_state_and_fail_all(ZeroDivisionError())

    f = GCFuture(gc_task_id=task_uuid)
    gce.submit(task_f=f, resource_specification={}, packed_task=b"MOCK_TASK")

    r = messagepack.unpack(f.result())
    assert isinstance(r, Result)
    assert ZeroDivisionError.__name__ in r.data
    assert r.task_id == task_uuid
    assert r.error_details.code == "RemoteExecutionError"


def test_gcengine_rejects_mpi_mode(randomstring):
    with pytest.raises(ValueError) as pyt_exc_1:
        GlobusComputeEngine(enable_mpi_mode=True, address="::1")

    assert "is not supported" in str(pyt_exc_1)

    with pytest.raises(ValueError) as pyt_exc_2:
        GlobusComputeEngine(mpi_launcher=randomstring(), address="::1")

    assert "is not supported" in str(pyt_exc_2)


def test_gcengine_rejects_resource_specification(task_uuid, gce):
    gce._engine_ready = True
    f = GCFuture(gc_task_id=task_uuid)
    gce.submit(f, packed_task=b"packed_task", resource_specification={"foo": "bar"})
    r = messagepack.unpack(f.result())
    assert isinstance(r, Result)
    assert r.is_error
    assert "is not supported" in r.data


def test_gcmpiengine_default_executor(randomstring):
    with mock.patch.object(GlobusMPIEngine, "_ExecutorClass") as mock_ex:
        mock_ex.__name__ = randomstring()
        mock_ex.return_value = mock.Mock(launch_cmd="")
        engine = GlobusMPIEngine()

    assert mock_ex.called, "Expect Parsl's MPIExecutor as default option"

    exp_en_label = GlobusMPIEngine.__name__
    exp_ex_label = f"{exp_en_label}-{mock_ex.__name__}"
    assert engine.label == exp_en_label, "Expect reasonable, explanatory default"

    engine.shutdown()
    a, k = mock_ex.call_args
    assert k["label"] == exp_ex_label, "Expect distinct executor label"
    assert k["encrypted"] is True, "Expect encrypted by default"


def test_gcmpiengine_accepts_resource_specification(task_uuid, randomstring):
    spec = {"some": "conf", "sentinel": randomstring()}
    with mock.patch.object(GlobusMPIEngine, "_ExecutorClass") as mock_ex:
        mock_ex.__name__ = "ClassName"
        mock_ex.return_value = mock.Mock(launch_cmd="")
        engine = GlobusMPIEngine(address="::1")
        engine._engine_ready = True
        f = GCFuture(gc_task_id=task_uuid)
        engine.submit(f, b"some task", resource_specification=spec)

    assert engine.executor.submit.called, "Verify test: correct internal method invoked"
    engine.shutdown()

    a, _k = engine.executor.submit.call_args
    assert spec in a
