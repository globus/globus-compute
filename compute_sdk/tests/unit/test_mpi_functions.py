import os

import pytest
from globus_compute_sdk.sdk.mpi_function import MPIFunction


@pytest.fixture
def run_in_tmp_dir(tmp_path):
    os.chdir(tmp_path)
    return tmp_path


def test_mpi_function(run_in_tmp_dir):
    """Confirm that the MPIFunction applies an MPI prefix"""
    mpi_func = MPIFunction("hostname")
    bash_result = mpi_func()

    assert "$PARSL_MPI_PREFIX hostname" in bash_result.cmd
