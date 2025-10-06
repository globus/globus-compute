import keyword
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

    def __str__(self) -> str:
        rc = self.returncode
        _sout = self.stdout.lstrip("\n").rstrip()
        sout = "\n".join(_sout[-1024:].splitlines()[-10:])
        if sout != _sout:
            sout = f"[... truncated; see .stdout for full output ...]\n{sout}"
        msg = f"exit code: {rc}\n   stdout:\n{sout}"
        if rc != 0:
            # not successful
            _serr = self.stderr.lstrip("\n").rstrip()
            serr = "\n".join(_serr[-1024:].splitlines()[-10:])
            if serr != _serr:
                serr = f"[... truncated; see .stderr for full output ...]\n{serr}"
            msg += f"\n\n   stderr:\n{serr}"
            if self.exception_name:
                msg += f"\n\nexception: {self.exception_name}"
        return msg

    def __repr__(self) -> str:
        parts = [
            f"returncode={self.returncode!r}",
            f"cmd={self.cmd!r}",
        ]
        if self.exception_name is not None:
            parts.append(f"exception_name={self.exception_name!r}")

        parts.extend((f"stdout={self.stdout!r}", f"stderr={self.stderr!r}"))
        k = ", ".join(parts)
        return f"{type(self).__name__}({k})"


class ShellFunction:

    def __init__(
        self,
        cmd: str,
        stdout: t.Optional[str] = None,
        stderr: t.Optional[str] = None,
        walltime: t.Optional[float] = None,
        snippet_lines=1000,
        name: t.Optional[str] = None,
        return_dict: bool = False,
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

        name: str | None
            Name used to register function. Defaults to class name ShellFunction
            if not set.

        return_dict: bool
            Indicates whether the function will return a JSON-compatible dict (`True`),
            or a `ShellResult` object (`False`).
        """
        self.cmd = cmd
        self.stdout = stdout
        self.stderr = stderr
        self.walltime = walltime
        if walltime:
            assert walltime >= 0, f"Negative walltime={walltime} is not allowed"
        self.snippet_lines = snippet_lines

        name = name or type(self).__name__
        assert isinstance(name, str)
        self.__name__ = self.valid_function_name(name)

        self.return_dict = return_dict

    def __str__(self) -> str:
        parts = []
        if self.walltime is not None:
            parts.append(f" wall time: {self.walltime}")
        if self.stdout is not None:
            parts.append(f"    stdout: {self.stdout!r}")
        if self.stderr is not None:
            parts.append(f"    stderr: {self.stderr!r}")
        parts.append(f"     lines: {self.snippet_lines!r}")

        command_header = "   command: "
        _cmd = self.cmd.strip()
        cmd = "\n".join(_cmd[:1024].splitlines()[:10])
        if cmd != _cmd:
            cmd += "\n[... truncated; see .cmd for full script ...]"
        if "\n" in cmd:
            indent = "\n" + len(command_header) * " "
            cmd = cmd.strip().replace("\n", indent)
            cmd = f"-----{indent}{cmd}{indent}-----"

        parts.append(f"{command_header}{cmd}")
        parts_str = "\n".join(parts)
        return f"{self.__name__}\n{parts_str}"

    def __repr__(self) -> str:
        parts = []
        if self.__name__ != type(self).__name__:
            parts.append(f"name={self.__name__!r}")
        if self.walltime is not None:
            parts.append(f"walltime={self.walltime!r}")
        if self.stdout is not None:
            parts.append(f"stdout={self.stdout!r}")
        if self.stderr is not None:
            parts.append(f"stderr={self.stderr!r}")
        parts.append(f"snippet_lines={self.snippet_lines}")
        parts.append(f"cmd={self.cmd!r}")
        k = ", ".join(parts)
        return f"{type(self).__name__}({k})"

    def valid_function_name(self, v: str) -> str:
        if not (v.isidentifier() and not keyword.iskeyword(v)):
            raise ValueError("Function name must be valid python name")
        return v

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
    ) -> t.Union[ShellResult, dict]:
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

        result = {
            "cmd": cmd,
            "stdout": stdout_snippet,
            "stderr": stderr_snippet,
            "returncode": returncode,
            "exception_name": exception_name,
        }
        if self.return_dict:
            return result
        return ShellResult(**result)  # type: ignore[arg-type]

    def __call__(
        self,
        **kwargs,
    ) -> t.Union[ShellResult, dict]:
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
        dict
            Shell result data as a JSON-compatible dict
        """
        cmd_line = self.cmd.format(**kwargs)
        return self.execute_cmd_line(cmd_line)
