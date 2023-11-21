import logging
import uuid
from unittest import mock

import pytest
from globus_compute_endpoint.engines.globus_compute import GlobusComputeEngine


def test_docker(tmp_path, uri="funcx/kube-endpoint:main-3.10"):
    gce = GlobusComputeEngine(
        address="127.0.0.1", container_type="docker", container_uri=uri
    )

    gce.executor.start = mock.MagicMock()
    gce.start(endpoint_id=uuid.uuid4(), run_dir=tmp_path)

    assert gce.executor.launch_cmd
    assert gce.executor.launch_cmd.startswith("docker run")
    assert uri in gce.executor.launch_cmd
    gce.executor.start.assert_called()
    # No cleanup necessary because HTEX was not started


def test_apptainer(tmp_path, uri="/tmp/kube-endpoint.sif"):
    gce = GlobusComputeEngine(
        address="127.0.0.1", container_type="apptainer", container_uri=uri
    )

    gce.executor.start = mock.MagicMock()
    gce.start(endpoint_id=uuid.uuid4(), run_dir=tmp_path)

    assert gce.executor.launch_cmd
    assert gce.executor.launch_cmd.startswith("apptainer run")
    assert uri in gce.executor.launch_cmd
    gce.executor.start.assert_called()


def test_custom(tmp_path):
    gce = GlobusComputeEngine(
        address="127.0.0.1",
        container_type="custom",
        container_cmd_options=(
            "mycontainer -v {EXECUTOR_RUNDIR}:"
            "{EXECUTOR_RUNDIR} {EXECUTOR_LAUNCH_CMD}"
        ),
    )

    gce.executor.start = mock.MagicMock()
    gce.start(endpoint_id=uuid.uuid4(), run_dir=tmp_path)
    logging.warning(f"Got launch {gce.executor.launch_cmd}")

    assert gce.executor.launch_cmd
    assert gce.executor.launch_cmd.startswith("mycontainer")
    assert f"{tmp_path}:{tmp_path}" in gce.executor.launch_cmd
    assert "process_worker_pool.py" in gce.executor.launch_cmd
    gce.executor.start.assert_called()


def test_bad_container(tmp_path):
    with pytest.raises(AssertionError):
        GlobusComputeEngine(address="127.0.0.1", container_type="BAD")


def test_no_container(tmp_path):
    gce = GlobusComputeEngine(address="127.0.0.1")
    original = gce.executor.launch_cmd

    gce.executor.start = mock.MagicMock()
    gce.start(endpoint_id=uuid.uuid4(), run_dir=tmp_path)

    assert gce.executor.launch_cmd == original
