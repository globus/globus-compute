import concurrent.futures
import random

import pytest
from globus_compute_common import messagepack
from globus_compute_endpoint.engines import GCFuture, GlobusMPIEngine
from globus_compute_sdk.sdk.mpi_function import MPIFunction
from globus_compute_sdk.sdk.shell_function import ShellResult
from parsl.executors.errors import InvalidResourceSpecification
from tests.utils import get_env_vars


def test_mpi_function(engine_runner, nodeslist, serde, task_uuid, ez_pack_task):
    """Test for the right cmd being generated"""
    engine = engine_runner(GlobusMPIEngine)

    num_nodes = random.randint(1, len(nodeslist))
    resource_spec = {"num_nodes": num_nodes, "num_ranks": random.randint(1, 4)}

    mpi_func = MPIFunction("pwd")
    task_bytes = ez_pack_task(mpi_func)
    future = GCFuture(task_uuid)
    engine.submit(future, task_bytes, resource_specification=resource_spec)

    packed_result = future.result(timeout=10)
    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.task_id == task_uuid
    assert result.error_details is None
    result = serde.deserialize(result.data)

    assert isinstance(result, ShellResult)
    assert result.cmd == "$PARSL_MPI_PREFIX pwd"


def test_env_vars(engine_runner, nodeslist, serde, task_uuid, ez_pack_task):
    engine = engine_runner(GlobusMPIEngine)
    task_bytes = ez_pack_task(get_env_vars)
    num_nodes = random.randint(1, len(nodeslist))
    future = GCFuture(task_uuid)
    engine.submit(
        future,
        task_bytes,
        resource_specification={"num_nodes": num_nodes, "num_ranks": 2},
    )

    packed_result = future.result(timeout=10)
    result = messagepack.unpack(packed_result)
    assert isinstance(result, messagepack.message_types.Result)
    assert result.task_id == task_uuid
    assert result.error_details is None
    inner_result = serde.deserialize(result.data)

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
