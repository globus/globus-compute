import concurrent.futures
import random
import uuid

import pytest
from globus_compute_common import messagepack
from globus_compute_endpoint.engines import GlobusMPIEngine
from globus_compute_sdk.sdk.mpi_function import MPIFunction
from globus_compute_sdk.sdk.shell_function import ShellResult
from globus_compute_sdk.serialize import ComputeSerializer
from parsl.executors.high_throughput.mpi_prefix_composer import (
    InvalidResourceSpecification,
)
from tests.utils import ez_pack_function, get_env_vars


def test_mpi_function(engine_runner, nodeslist, tmp_path):
    """Test for the right cmd being generated"""
    engine = engine_runner(GlobusMPIEngine)
    task_id = uuid.uuid1()
    serializer = ComputeSerializer()

    num_nodes = random.randint(1, len(nodeslist))
    resource_spec = {"num_nodes": num_nodes, "num_ranks": random.randint(1, 4)}

    mpi_func = MPIFunction("pwd")
    task_body = ez_pack_function(serializer, mpi_func, (), {})
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

    assert isinstance(result, ShellResult)
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


def test_mpi_res_spec(engine_runner, nodeslist):
    """Test passing valid MPI Resource spec to mpi enabled engine"""
    engine = engine_runner(GlobusMPIEngine)

    for i in range(1, len(nodeslist) + 1):
        res_spec = {"num_nodes": i, "num_ranks": random.randint(1, 8)}

        future = engine._submit(get_env_vars, resource_specification=res_spec)
        assert isinstance(future, concurrent.futures.Future)
        env_vars = future.result()
        provisioned_nodes = env_vars["PARSL_MPI_NODELIST"].strip().split(",")

        assert len(provisioned_nodes) == res_spec["num_nodes"]
        for prov_node in provisioned_nodes:
            assert prov_node in nodeslist
        assert env_vars["PARSL_NUM_NODES"] == str(res_spec["num_nodes"])
        assert env_vars["PARSL_NUM_RANKS"] == str(res_spec["num_ranks"])
        assert env_vars["PARSL_MPI_PREFIX"].startswith("mpiexec")


def test_no_mpi_res_spec(engine_runner):
    """Test passing valid MPI Resource spec to mpi enabled engine"""
    engine = engine_runner(GlobusMPIEngine)

    res_spec = None
    with pytest.raises(AttributeError):
        engine._submit(get_env_vars, resource_specification=res_spec)


def test_bad_mpi_res_spec(engine_runner):
    """Test passing valid MPI Resource spec to mpi enabled engine"""
    engine = engine_runner(GlobusMPIEngine)

    res_spec = {"FOO": "BAR"}

    with pytest.raises(InvalidResourceSpecification):
        engine._submit(get_env_vars, resource_specification=res_spec)
