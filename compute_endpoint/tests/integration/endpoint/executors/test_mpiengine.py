import random
import uuid
from queue import Queue

import pytest
from globus_compute_common import messagepack
from globus_compute_endpoint.engines import GlobusMPIEngine
from globus_compute_sdk.sdk.bash_function import BashResult
from globus_compute_sdk.sdk.mpi_function import MPIFunction
from globus_compute_sdk.serialize import ComputeSerializer
from parsl.launchers import SimpleLauncher
from parsl.providers import LocalProvider
from tests.utils import ez_pack_function, get_env_vars


@pytest.fixture
def nodeslist(num=100):
    limit = random.randint(1, num)
    yield [f"NODE{node_i}" for node_i in range(1, limit)]


@pytest.fixture
def mpi_engine(tmp_path, nodeslist):
    ep_id = uuid.uuid4()

    nodefile_path = tmp_path / "pbs_nodefile"
    nodes_string = "\n".join(nodeslist)
    worker_init = f"""
    echo -e "{nodes_string}" > {nodefile_path} ;
    export PBS_NODEFILE={nodefile_path}
    """

    engine = GlobusMPIEngine(
        address="127.0.0.1",
        heartbeat_period=1,
        heartbeat_threshold=1,
        max_retries_on_system_failure=0,
        mpi_launcher="mpiexec",
        provider=LocalProvider(
            init_blocks=1,
            min_blocks=1,
            max_blocks=1,
            worker_init=worker_init,
            launcher=SimpleLauncher(),
        ),
    )
    engine._status_report_thread.reporting_period = 1
    queue = Queue()
    engine.start(endpoint_id=ep_id, run_dir=tmp_path, results_passthrough=queue)
    yield engine, nodeslist
    engine.shutdown()


def test_mpi_function(mpi_engine, tmp_path):
    """Test for the right cmd being generated"""
    engine, nodeslist = mpi_engine
    queue = engine.results_passthrough
    task_id = uuid.uuid1()
    serializer = ComputeSerializer()

    num_nodes = random.randint(1, len(nodeslist))
    resource_spec = {"num_nodes": num_nodes, "num_ranks": random.randint(1, 4)}

    mpi_func = MPIFunction("pwd", resource_specification=resource_spec)
    task_body = ez_pack_function(
        serializer, mpi_func, (), {"resource_specification": resource_spec}
    )
    task_message = messagepack.pack(
        messagepack.message_types.Task(task_id=task_id, task_buffer=task_body)
    )
    engine.submit(task_id, task_message, resource_specification=resource_spec)
    flag = False
    for _i in range(20):
        q_msg = queue.get(timeout=10)
        assert isinstance(q_msg, dict)

        packed_result_q = q_msg["message"]
        result = messagepack.unpack(packed_result_q)
        if isinstance(result, messagepack.message_types.Result):
            assert result.task_id == task_id
            assert result.error_details is None
            result = serializer.deserialize(result.data)

            assert isinstance(result, BashResult)
            assert result.cmd == "$PARSL_MPI_PREFIX pwd"
            flag = True
            break

    assert flag, "Expected result packet, but none received"


def test_env_vars(mpi_engine, tmp_path):
    engine, nodeslist = mpi_engine
    queue = engine.results_passthrough
    task_id = uuid.uuid1()
    serializer = ComputeSerializer()
    task_body = ez_pack_function(serializer, get_env_vars, (), {})

    task_message = messagepack.pack(
        messagepack.message_types.Task(task_id=task_id, task_buffer=task_body)
    )
    num_nodes = random.randint(1, len(nodeslist))
    engine.submit(
        task_id,
        task_message,
        resource_specification={"num_nodes": num_nodes, "num_ranks": 2},
    )

    flag = False
    for _i in range(20):
        q_msg = queue.get(timeout=10)
        assert isinstance(q_msg, dict)

        packed_result_q = q_msg["message"]
        result = messagepack.unpack(packed_result_q)
        if isinstance(result, messagepack.message_types.Result):
            assert result.task_id == task_id
            assert result.error_details is None
            result = serializer.deserialize(result.data)
            assert result["PARSL_NUM_NODES"] == str(num_nodes)
            assert result["PARSL_MPI_PREFIX"].startswith("mpiexec")
            flag = True
            break

    assert flag, "Expected result packet, but none received"
