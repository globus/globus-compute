import pytest
from globus_compute_sdk.sdk.mpi_function import MPIFunction


def test_mpi_function(run_in_tmp_dir):
    """Confirm that the MPIFunction applies an MPI prefix"""
    mpi_func = MPIFunction("hostname")
    bash_result = mpi_func()

    assert "$PARSL_MPI_PREFIX hostname" in bash_result.cmd


def test_mpi_function_name_default(run_in_tmp_dir):
    mpi_func = MPIFunction("hostname")
    assert mpi_func.__name__ == type(mpi_func).__name__


@pytest.mark.parametrize("fn_name", ["lammps", "my_mpi_app"])
def test_mpi_function_name(run_in_tmp_dir, fn_name):
    mpi_func = MPIFunction("hostname", name=fn_name)
    assert mpi_func.__name__ == fn_name
