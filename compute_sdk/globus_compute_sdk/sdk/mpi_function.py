import typing as t

from .shell_function import ShellFunction, ShellResult


class MPIFunction(ShellFunction):
    """MPIFunction extends ShellFunction, as a thin wrapper that adds an
    MPI launcher prefix to the BashFunction command.
    """

    def __init__(
        self,
        cmd: str,
        stdout: t.Optional[str] = None,
        stderr: t.Optional[str] = None,
        walltime: t.Optional[float] = None,
        snippet_lines=1000,
    ):
        """Initialize a ShellFunction

        Parameters
        ----------
        cmd: str
             formattable command line to execute. For e.g:
             "lammps -in {input_file}" where {input_file} is formatted
             with kwargs passed at call time.

        stdout: str | None
            file path to which stdout should be captured

        stderr: str | None
            file path to which stderr should be captures

        walltime: float | None
            duration in seconds after which the command should be interrupted

        snippet_lines: int
            Number of lines of stdout/err to capture,
            default=1000

        """
        self.cmd = cmd
        self.stdout = stdout
        self.stderr = stderr
        self.walltime = walltime
        self.snippet_lines = snippet_lines

    def __call__(
        self,
        **kwargs,
    ) -> ShellResult:
        """This method is passed from an executor to an endpoint to execute the
        ShellFunction :

        .. code-block:: python

             bf = ShellFunction("echo 'Hello'")
             future = executor.submit(bf)  # Invokes this method on an endpoint
             future.result()               # returns a ShellResult

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
