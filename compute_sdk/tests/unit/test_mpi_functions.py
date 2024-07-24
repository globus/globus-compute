from globus_compute_sdk.sdk.mpi_function import MPIFunction


def test_mpi_function(run_in_tmp_dir):
    """Confirm that the MPIFunction applies an MPI prefix"""
    mpi_func = MPIFunction("hostname")
    bash_result = mpi_func()

    assert "$PARSL_MPI_PREFIX hostname" in bash_result.cmd
