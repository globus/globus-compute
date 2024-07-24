import random
import uuid

import pytest
from globus_compute_common import messagepack
from globus_compute_endpoint.engines import GlobusMPIEngine
from globus_compute_sdk.serialize import ComputeSerializer
from tests.utils import ez_pack_function, get_env_vars

temporary_skip = False
try:
    from globus_compute_sdk.sdk.bash_function import BashResult
    from globus_compute_sdk.sdk.mpi_function import MPIFunction
except ImportError:
    temporary_skip = True


@pytest.mark.skipif(temporary_skip, reason="Skip test until MPIFunctions are merged")
def test_mpi_function(engine_runner, nodeslist, tmp_path):
    """Test for the right cmd being generated"""
    engine = engine_runner(GlobusMPIEngine)
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
    future = engine.submit(task_id, task_message, resource_specification=resource_spec)

    packed_result = future.result(timeout=10)
    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.task_id == task_id
    assert result.error_details is None
    result = serializer.deserialize(result.data)

    assert isinstance(result, BashResult)
    assert result.cmd == "$PARSL_MPI_PREFIX pwd"


def test_env_vars(engine_runner, nodeslist, tmp_path):
    engine = engine_runner(GlobusMPIEngine)
    task_id = uuid.uuid1()
    serializer = ComputeSerializer()
    task_body = ez_pack_function(serializer, get_env_vars, (), {})

    task_message = messagepack.pack(
        messagepack.message_types.Task(task_id=task_id, task_buffer=task_body)
    )
    num_nodes = random.randint(1, len(nodeslist))
    future = engine.submit(
        task_id,
        task_message,
        resource_specification={"num_nodes": num_nodes, "num_ranks": 2},
    )

    packed_result = future.result(timeout=10)
    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.task_id == task_id
    assert result.error_details is None
    inner_result = serializer.deserialize(result.data)

    assert inner_result["PARSL_NUM_NODES"] == str(num_nodes)
    assert inner_result["PARSL_MPI_PREFIX"].startswith("mpiexec")
