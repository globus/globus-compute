import concurrent.futures
import logging
import random
import uuid
from queue import Queue

import pytest
from globus_compute_endpoint.engines import GlobusComputeEngine
from parsl.executors.high_throughput.mpi_prefix_composer import (
    InvalidResourceSpecification,
)
from parsl.launchers import SimpleLauncher
from parsl.providers import LocalProvider
from tests.utils import get_env_vars

logger = logging.getLogger(__name__)


@pytest.fixture
def nodeslist(num=100):
    limit = random.randint(1, num)
    yield [f"NODE{node_i}" for node_i in range(1, limit)]


@pytest.fixture
def gc_engine_with_mpi(tmp_path, nodeslist):
    ep_id = uuid.uuid4()

    nodefile_path = tmp_path / "pbs_nodefile"
    nodes_string = "\n".join(nodeslist)
    worker_init = f"""
    echo -e "{nodes_string}" > {nodefile_path} ;
    export PBS_NODEFILE={nodefile_path}
    """
    engine = GlobusComputeEngine(
        address="127.0.0.1",
        heartbeat_period=1,
        heartbeat_threshold=2,
        enable_mpi_mode=True,
        mpi_launcher="mpiexec",
        provider=LocalProvider(
            init_blocks=1,
            min_blocks=1,
            max_blocks=1,
            worker_init=worker_init,
            launcher=SimpleLauncher(),
        ),
        strategy="none",
        job_status_kwargs={"max_idletime": 0, "strategy_period": 0.1},
    )
    queue = Queue()
    engine.start(endpoint_id=ep_id, run_dir=str(tmp_path), results_passthrough=queue)

    yield engine, nodeslist
    engine.shutdown()


def test_mpi_res_spec(gc_engine_with_mpi):
    """Test passing valid MPI Resource spec to mpi enabled engine"""
    engine, nodes = gc_engine_with_mpi

    for i in range(1, len(nodes) + 1):
        res_spec = {"num_nodes": i, "num_ranks": random.randint(1, 8)}

        future = engine._submit(get_env_vars, resource_specification=res_spec)
        assert isinstance(future, concurrent.futures.Future)
        env_vars = future.result()
        provisioned_nodes = env_vars["PARSL_MPI_NODELIST"].strip().split(",")

        assert len(provisioned_nodes) == res_spec["num_nodes"]
        for prov_node in provisioned_nodes:
            assert prov_node in nodes
        assert env_vars["PARSL_NUM_NODES"] == str(res_spec["num_nodes"])
        assert env_vars["PARSL_NUM_RANKS"] == str(res_spec["num_ranks"])
        assert env_vars["PARSL_MPI_PREFIX"].startswith("mpiexec")


def test_no_mpi_res_spec(gc_engine_with_mpi):
    """Test passing valid MPI Resource spec to mpi enabled engine"""
    engine, nodes = gc_engine_with_mpi

    res_spec = None
    with pytest.raises(AttributeError):
        engine._submit(get_env_vars, resource_specification=res_spec)


def test_bad_mpi_res_spec(gc_engine_with_mpi):
    """Test passing valid MPI Resource spec to mpi enabled engine"""
    engine, nodes = gc_engine_with_mpi

    res_spec = {"FOO": "BAR"}

    with pytest.raises(InvalidResourceSpecification):
        engine._submit(get_env_vars, resource_specification=res_spec)
