import typing as t


class ShellResult:

    def __init__(
        self,
        cmd: str,
        stdout: str,
        stderr: str,
        returncode: int,
        exception_name: t.Optional[str] = None,
    ):
        """

        Parameters
        ----------
        cmd: str
            formatted command line string that was executed on the endpoint

        stdout: str
            multiline (default 1K) snippet of stdout

        stderr: str
            multiline (default 1K) snippet of stderr

        returncode: int
            Return code from command execution

        exception_name
        """
        self.cmd = cmd
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode
        self.exception_name = exception_name

    def __str__(self):
        return f"Command {self.cmd} returned with exit status: {self.returncode}"

    def __repr__(self):
        return (
            f"{self.__class__.__name__}(returncode={self.returncode}, cmd={self.cmd})"
        )


class ShellFunction:

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
        if walltime:
            assert walltime >= 0, f"Negative walltime={walltime} is not allowed"
        self.snippet_lines = snippet_lines

    @property
    def __name__(self):
        # This is required for function registration
        return f"{self.__class__.__name__}: {self.cmd}"

    def open_std_fd(self, fname, mode: str = "a+"):
        import os

        # fname is 'stdout' or 'stderr'
        if fname is None:
            return None

        if os.path.dirname(fname):
            os.makedirs(os.path.dirname(fname), exist_ok=True)
        fd = open(fname, mode)
        return fd

    def get_snippet(self, file_obj) -> str:
        file_obj.seek(0, 0)
        last_n_lines = file_obj.readlines()[-self.snippet_lines :]
        return "".join(last_n_lines)

    def get_and_close_streams(self, stdout, stderr) -> t.Tuple[str, str]:
        stdout_snippet = self.get_snippet(stdout)
        stderr_snippet = self.get_snippet(stderr)
        stdout.close()
        stderr.close()
        return stdout_snippet, stderr_snippet

    def execute_cmd_line(
        self,
        cmd: str,
    ) -> ShellResult:
        import os
        import subprocess
        import tempfile
        import uuid

        sandbox_error_message = None

        run_dir = None
        # run_dir takes priority over sandboxing
        if os.environ.get("GC_TASK_SANDBOX_DIR"):
            run_dir = os.environ["GC_TASK_SANDBOX_DIR"]
        else:
            sandbox_error_message = (
                "WARNING: Task sandboxing will not work due to "
                "endpoint misconfiguration. Please enable sandboxing "
                "on the remote endpoint. "
            )

        if run_dir:
            os.makedirs(run_dir, exist_ok=True)
            os.chdir(run_dir)

        # For backward compatibility with older endpoints which don't export
        # GC_TASK_UUID, a truncated uuid string will be used as prefix
        prefix = os.environ.get("GC_TASK_UUID", str(uuid.uuid4())[-10:]) + "."

        stdout = (
            self.stdout
            or tempfile.NamedTemporaryFile(
                dir=os.getcwd(), prefix=prefix, suffix=".stdout"
            ).name
        )
        stderr = (
            self.stderr
            or tempfile.NamedTemporaryFile(
                dir=os.getcwd(), prefix=prefix, suffix=".stderr"
            ).name
        )
        std_out = self.open_std_fd(stdout)
        std_err = self.open_std_fd(stderr)
        exception_name = None

        if sandbox_error_message:
            print(sandbox_error_message, file=std_err)

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=std_out,
                stderr=std_err,
                shell=True,
                executable="/bin/bash",
                close_fds=False,
            )
            proc.wait(timeout=self.walltime)
            returncode = proc.returncode
            if returncode != 0:
                exception_name = "subprocess.CalledProcessError"

        except subprocess.TimeoutExpired:
            # Returncode to match behavior of timeout bash command
            # https://man7.org/linux/man-pages/man1/timeout.1.html
            returncode = 124
            exception_name = "subprocess.TimeoutExpired"

        finally:
            stdout_snippet, stderr_snippet = self.get_and_close_streams(
                std_out, std_err
            )

        return ShellResult(
            cmd,
            stdout_snippet,
            stderr_snippet,
            returncode,
            exception_name=exception_name,
        )

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
        cmd_line = self.cmd.format(**kwargs)
        return self.execute_cmd_line(cmd_line)
