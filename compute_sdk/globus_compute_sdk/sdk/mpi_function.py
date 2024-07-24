from .shell_function import ShellFunction, ShellResult


class MPIFunction(ShellFunction):
    """MPIFunction extends ShellFunction, as a thin wrapper that adds an
    MPI launcher prefix to the BashFunction command.
    """

    def __call__(
        self,
        **kwargs,
    ) -> ShellResult:
        """This method is passed from an executor to an endpoint to execute the
        MPIFunction :

        .. code-block:: python

             mpi_func = MPIFunction("echo 'Hello'")
             executor.resource_specification = {"num_nodes": 2, "ranks_per_node": 2}
             future = executor.submit(mpi_func)  # Invokes this method on an endpoint
             future.result()                     # returns a ShellResult

        Parameters
        ----------

        **kwargs:
            arbitrary keyword args will be used to format the `cmd` string
            before execution

        Returns
        -------

        ShellResult: ShellResult
            Shell result object that encapsulates outputs from
            command execution
        """
        cmd_line = "$PARSL_MPI_PREFIX " + self.cmd.format(**kwargs)
        return self.execute_cmd_line(cmd_line)
