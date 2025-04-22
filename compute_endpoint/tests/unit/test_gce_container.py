import random
import typing as t
import uuid
from unittest import mock

import pytest
from globus_compute_endpoint.engines.globus_compute import GlobusComputeEngine

_MOCK_BASE = "globus_compute_endpoint.engines.globus_compute."
_LAUNCH_CMD_PREFIX = (
    "globus-compute-endpoint python-exec"
    " parsl.executors.high_throughput.process_worker_pool"
)


@pytest.fixture
def gce_factory(tmp_path, randomstring) -> t.Callable:
    engines: list[GlobusComputeEngine] = []

    def _kernel(**k):
        expect_uri = randomstring(length=random.randint(1, 20))
        expect_opts = randomstring(length=random.randint(1, 20))
        k = {
            "address": "::1",
            "max_workers_per_node": 1,
            "label": "GCE_TEST",
            "container_uri": expect_uri,
            "container_cmd_options": expect_opts,
            **k,
        }
        with mock.patch(f"{_MOCK_BASE}JobStatusPoller"):
            gce = GlobusComputeEngine(**k)
            gce.executor.start = mock.Mock()
            gce.start(endpoint_id=uuid.uuid4(), run_dir=str(tmp_path))
            assert gce.executor.start.called

            engines.append(gce)

        return gce, expect_uri, expect_opts

    yield _kernel

    for e in engines:
        e.shutdown()


def test_docker(tmp_path, gce_factory):
    gce, exp_uri, exp_opts = gce_factory(container_type="docker")
    container_launch_cmd = gce.executor.launch_cmd
    expected = (
        f"docker run {exp_opts} -v {tmp_path}:{tmp_path} -t"
        f" {exp_uri} {_LAUNCH_CMD_PREFIX}"
    )
    assert container_launch_cmd.startswith(expected)


def test_apptainer(gce_factory):
    gce, exp_uri, exp_opts = gce_factory(container_type="apptainer")
    container_launch_cmd = gce.executor.launch_cmd
    expected = f"apptainer run {exp_opts} {exp_uri} {_LAUNCH_CMD_PREFIX}"
    assert container_launch_cmd.startswith(expected)


def test_singularity(gce_factory, randomstring):
    gce, exp_uri, exp_opts = gce_factory(container_type="singularity")
    container_launch_cmd = gce.executor.launch_cmd
    expected = f"singularity run {exp_opts} {exp_uri} {_LAUNCH_CMD_PREFIX}"
    assert container_launch_cmd.startswith(expected)


def test_podman(tmp_path, gce_factory):
    gce, exp_uri, exp_opts = gce_factory(container_type="podman")
    container_launch_cmd = gce.executor.launch_cmd
    expected = (
        f"podman run {exp_opts} -v {tmp_path}:{tmp_path} -t"
        f" {exp_uri} {_LAUNCH_CMD_PREFIX}"
    )
    assert container_launch_cmd.startswith(expected)

def test_podman_hpx(tmp_path, gce_factory):
    gce, exp_uri, exp_opts = gce_factory(container_type="podman-hpc")
    container_launch_cmd = gce.executor.launch_cmd
    expected = (
        f"podman-hpc run {exp_opts} -v {tmp_path}:{tmp_path} -t"
        f" {exp_uri} {_LAUNCH_CMD_PREFIX}"
    )
    assert container_launch_cmd.startswith(expected)

def test_custom_missing_options(tmp_path):
    gce = GlobusComputeEngine(
        address="::1", max_workers_per_node=1, label="GCE_TEST", container_type="custom"
    )
    with pytest.raises(AssertionError) as pyt_e:
        gce.start(endpoint_id=uuid.uuid4(), run_dir=tmp_path)
    gce.shutdown()
    assert "container_cmd_options is required" in str(pyt_e.value)


def test_custom(gce_factory, randomstring):
    exp_exec = randomstring()
    gce, *_ = gce_factory(
        container_type="custom",
        container_uri=None,  # not necessary, but not used; "undo" test factory default
        container_cmd_options=f"{exp_exec} {{EXECUTOR_RUNDIR}} {{EXECUTOR_LAUNCH_CMD}}",
    )
    container_launch_cmd = gce.executor.launch_cmd
    expected = f"{exp_exec} {gce.run_dir} {_LAUNCH_CMD_PREFIX}"
    assert container_launch_cmd.startswith(expected)


def test_bad_container():
    with pytest.raises(AssertionError):
        GlobusComputeEngine(address="::1", container_type="BAD")
