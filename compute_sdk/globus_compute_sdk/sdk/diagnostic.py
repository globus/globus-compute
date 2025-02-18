from __future__ import annotations

import argparse
import glob
import gzip
import io
import os
import platform
import shlex
import shutil
import socket
import ssl
import subprocess
import sys
from concurrent.futures import TimeoutError
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime as dt
from datetime import timezone
from urllib.parse import urlparse

import colorama
from globus_compute_sdk import Client, Executor
from globus_compute_sdk.sdk._environments import (
    get_amqp_service_host,
    get_web_service_url,
)
from globus_compute_sdk.sdk.client import _ComputeWebClient
from globus_compute_sdk.sdk.compute_dir import ensure_compute_dir
from globus_compute_sdk.sdk.executor import _RESULT_WATCHERS
from globus_compute_sdk.sdk.hardware_report import hardware_commands_list
from globus_compute_sdk.sdk.utils import display_name
from globus_compute_sdk.sdk.utils.sample_function import (
    sdk_tutorial_sample_simple_function,
)
from globus_compute_sdk.serialize.concretes import DillCodeTextInspect
from rich import get_console
from rich.console import Console

OUTPUT_FILENAME = "globus_compute_diagnostic_{}.txt.gz"
# How long to wait for any individual diagnostic before timing out
DIAG_TIMEOUT_S = 15


def cat(
    paths: list[str], wildcard: bool = False, max_bytes: int = 0, sort_dir: bool = True
):
    max_bytes = max(0, max_bytes)

    # The following segment expands all the paths using wildcards first,
    # then sorts them alphabetically so that directories are grouped together
    # before individually adding the files to the output
    all_paths = [os.path.expanduser(path) for path in paths]
    if wildcard:
        files = []
        for full_path in all_paths:
            files.extend(glob.glob(full_path, recursive=True))
    else:
        files = all_paths

    if sort_dir:
        files.sort()

    @display_name(f"cat({paths})")
    def kernel() -> None:
        for filename in files:
            cat_cmd = "cat " + filename
            print(f'{cat_cmd}\n{"=" * len(cat_cmd)}')
            if os.path.exists(filename):
                with open(filename, "rb") as f:
                    if max_bytes:
                        file_size = os.fstat(f.fileno()).st_size
                        f.seek(max(file_size - max_bytes, 0))
                    # replacing \ufffd with # to be more readable to keep
                    # the text a similar length
                    content = f.read().decode(errors="replace").replace("\ufffd", "#")
                    indented_content = content.replace("\n", "\n | ")
                print(f" | {indented_content}")
            else:
                print_warning(f"No file named {filename}\n")
            print(f'{"-" * len(cat_cmd)}\n')

    return kernel


def test_conn(host: str, port: int, timeout: int = 5):
    @display_name(f"Testing connectivity to {host}:{port}")
    def kernel():
        try:
            socket.create_connection((host, port), timeout=timeout)
            print(f"Connected successfully to {host} over port {port}!\n")
        except OSError as e:
            print_warning(f"Connection failed to {host} over port {port}: {e}\n")

    kernel.display_name = f"test_conn({host}, {port})"  # type: ignore
    return kernel


def test_ssl_conn(host: str, port: int, timeout: int = 5):
    @display_name(f"Testing SSL connection to {host}:{port}")
    def kernel() -> None:
        context = ssl.create_default_context()
        try:
            with socket.create_connection((host, port), timeout=timeout) as sock:
                with context.wrap_socket(sock, server_hostname=host) as ssock:
                    conn_data = ssock.cipher()
                    cipher, version, _ = conn_data if conn_data else (None, None, None)
            print(
                f"Successfully established SSL connection with {host}:{port}!\n"
                f"Version: {version}\n"
                f"Cipher:  {cipher}\n"
            )
        except OSError as e:
            print_warning(
                f"Failed to establish SSL connection with {host}:{port} - {e}\n",
            )

    kernel.display_name = f"test_ssl_conn({host}, {port})"  # type: ignore
    return kernel


def print_service_versions(base_url: str):
    @display_name(f"get_service_versions({base_url})")
    def kernel():
        # Just adds a newline to the version info for better formatting
        version_info = _ComputeWebClient(base_url=base_url).v2.get_version(
            service="all"
        )
        print(f"{version_info}\n")

    return kernel


def ifconfig_wrapper():
    if sys.platform in ["win32"]:
        return "ipconfig"

    return "ifconfig"


@display_name("OpenSSL version")
def print_openssl_version():
    print(f"{ssl.OPENSSL_VERSION}\n")


def print_warning(s: str):
    """
    Prints both to stderr (goes to console) and to stdout (captured into file)
    This method and print_highlight() below uses rich.Console.print() instead
    of the base rich.print() as no_wrap defaults to None for stdout, inserting
    line breaks when we don't want them (in tests for example)
    """
    Console(file=sys.stderr).print(f"[bold red]{s}[/bold red]", no_wrap=True)
    print(s)


def print_highlight(s: str):
    get_console().print(f"[bold yellow]{s}[/bold yellow]", no_wrap=True)


def os_info_cmd():
    if sys.platform == "linux":
        return cat(
            [
                "/etc/os-release",
            ]
        )
    elif sys.platform == "darwin":
        return "sw_vers"
    elif sys.platform == "win32":
        return "systeminfo"
    else:
        return None


def if_supported_on_os(cmd: str):
    """
    Misc commands that may exist for one or more platforms, but not for others.

    A simpler 'supported or not' check that returns None if unavailable
    """
    if cmd:
        BY_PLATFORM = {
            "ip": ["linux"],
            "lshw": ["linux"],
            "df": ["linux", "darwin"],
            "sysctl": ["linux", "darwin"],
            #  netstat may be slow on macOS, better to exclude it
            "netstat": ["linux", "win32"],
        }

        cmd_base = cmd.split()[0]
        if cmd_base in BY_PLATFORM:
            return cmd if sys.platform in BY_PLATFORM.get(cmd_base, []) else None
        else:
            # If not specifically named here, assume it's runnable on all
            return cmd
    else:
        return None


def filter_by_os(cmd_list: list):
    results = []
    for cmd in cmd_list:
        # if_supported_on_os only checks cmds in str form, not methods
        if cmd and (not isinstance(cmd, str) or if_supported_on_os(cmd)):
            results.append(cmd)

    return results


def print_endpoint_install_dir() -> str:
    # Works for bare install or pyenv.  May not work for pipx
    run_attempt = None
    try:
        run_attempt = subprocess.run("globus-compute-endpoint", capture_output=True)
    except Exception:
        # Treat errors as not found (FileNotFoundError raised on linux)
        pass

    if (
        run_attempt is None
        or bool(run_attempt.stderr)
        and "command not found" in run_attempt.stderr.decode()
    ):
        return "globus-compute-endpoint is not installed"
    else:
        # `which` by itself may return a value even if gce is not installed.
        # It will return the shim in pyenv, which contains info for other PY versions
        ep_path = shutil.which("globus-compute-endpoint")
        if ep_path:
            return f"globus-compute-endpoint is installed at {ep_path}"
        else:
            return "globus-compute-endpoint is not installed"


def connection_tests():
    web_svc_url = get_web_service_url()
    web_svc_host = urlparse(web_svc_url).netloc
    amqp_svc_host = get_amqp_service_host()

    return [
        test_conn(web_svc_host, 443),
        test_ssl_conn(web_svc_host, 443),
        test_conn(amqp_svc_host, 443),
        test_ssl_conn(amqp_svc_host, 443),
        test_conn(amqp_svc_host, 5671),
        test_ssl_conn(amqp_svc_host, 5671),
        print_service_versions(web_svc_url),
    ]


def general_commands_list():
    """
    General diagnostic commands
    Excludes endpoint hardware reports and sending tasks to endpoints
    """

    return [
        "uname -a",
        os_info_cmd(),
        "whoami",
        "pip freeze",
        print_openssl_version,
        platform.processor,
        ifconfig_wrapper(),
        if_supported_on_os("ip addr"),
        if_supported_on_os("ip route"),
        if_supported_on_os("netstat -r"),
    ]


def get_diagnostic_commands(log_bytes: int):

    commands = filter_by_os(hardware_commands_list())
    commands.extend(general_commands_list())
    commands.extend(connection_tests())

    gc_home_dir = ensure_compute_dir()

    # Run endpoint related diagnostics when it is installed
    ep_install_dir = print_endpoint_install_dir()
    if "is not installed" not in ep_install_dir:
        commands.extend(
            [
                print(ep_install_dir),
                "globus-compute-endpoint whoami",
                "globus-compute-endpoint list",
            ]
        )

        if log_bytes > 0:
            # Specifying 0 means we don't want to collect logs.
            # Do we want to disallow 0 for real world use?
            commands.append(
                cat(
                    [
                        f"{gc_home_dir}/*/*.yaml",
                        f"{gc_home_dir}/*/*.log",
                        f"{gc_home_dir}/*/*.py",
                        f"{gc_home_dir}/*/*.j2",
                        f"{gc_home_dir}/*/*.json",
                    ],
                    wildcard=True,
                    max_bytes=log_bytes,
                )
            )
    else:
        ep_install_dir = ""

    return commands, ep_install_dir


def run_single_command(cmd: str):
    cmd_list = shlex.split(cmd)
    arg0 = cmd_list[0]

    # special handling for redirection
    pipe_target = None
    pipe_parts = cmd.split("|")
    if len(pipe_parts) > 2:
        print_warning(f"Could not execute diagnostic command: {cmd}")
    elif len(pipe_parts) == 2:
        cmd_list = shlex.split(pipe_parts[0])
        pipe_target = shlex.split(pipe_parts[1])

    if not shutil.which(arg0):
        print_warning(f"{arg0} was not found in the PATH\n")
        return

    try:
        if pipe_target:
            first = subprocess.Popen(cmd_list, stdout=subprocess.PIPE)
            first.wait()
            res = subprocess.run(
                pipe_target,
                timeout=15,
                capture_output=True,
                stdin=first.stdout,
                text=True,
            )
        else:
            res = subprocess.run(
                cmd_list, timeout=DIAG_TIMEOUT_S, capture_output=True, text=True
            )
        if res.stdout:
            print(res.stdout)
        if res.stderr:
            print_warning(res.stderr)
    except subprocess.TimeoutExpired:
        print_warning("Command timed out\n")
    except Exception as e:
        print_warning(f"Command failed: {e}\n")


def run_all_diags_wrapper(
    print_only: bool, log_bytes: int, verbose: bool, ep_uuid: str | None = None
):
    """
    This is the diagnostic wrapper that obtains the list of commands to be run
    (which depends on whether we are in the SDK or Endpoint, and filters based
    on current platform OS), then runs the individual diagnostic methods one by
    one.  Output will be either to console, or if zipfile is True, redirected
    to a temporary per-test file then collated into a zip file upon completion.

    For each diagnostic, this method prints what is being run to stdout, then
    executes the diagnostic itself which outputs to stdout or file.

    :param print_only:        Whether to write diagnostic output to stdout
                                If True, diagnostic output is to stdout and no
                                local files are generated.
                                If False, only print the display_name of each
                                step as they are run to stdout to give the user
                                a better sense of progress (useful if there
                                are slower diagnostics), but capture all output
                                to a buffer and write a gzipped version of the
                                output to file in the working directory.
    :param log_bytes:         # of bytes to limit the individual endpoint log
                                extractions to
    :param verbose:           Whether to print test headings to stdout while
                                redirecting the output to compressed file
    :param ep_uuid:           # If specified, register a sample function and
                                send a task that uses the function UUID to the
                                endpoint to test end to end connectivity
    """

    current_time = dt.now().astimezone(timezone.utc).strftime("%Y-%m-%d-%H-%M-%SZ")
    zip_filename = f"{OUTPUT_FILENAME.format(current_time)}"

    diagnostic_output = []

    diag_cmds, ep_install_dir = get_diagnostic_commands(log_bytes)

    if ep_install_dir or ep_uuid:
        # If the Endpoint is installed we will be running ``whoami`` and ``list``
        # and if ep_uuid is provided we need to POST to the web-service.
        # That means we need to be logged in, and that will be blocked if we
        # are redirecting output (not a tty).  So we should trigger it early
        # by creating a Client
        print("Some diagnostic commands require being logged in.")
        # Use DillCodeTextInspect so the sample function code is easier to verify
        gcc = Client(code_serialization_strategy=DillCodeTextInspect())

    for cmd in diag_cmds:
        if cmd is None:
            # None signifies incompatibility with current OS with no warning needed
            # Other commands may also be unavailable, but will emit a warning
            continue
        elif callable(cmd):
            display_name = getattr(cmd, "display_name", f"{cmd.__name__}()")
            display_name = f"python:{display_name}"
        else:
            display_name = str(cmd)

        # This displays the command being run to console for every command
        # diagnostic_display = f"== Diagnostic: {display_name} =="
        diag_header = f"== Diagnostic: {display_name} =="
        if print_only or verbose:
            print_highlight(diag_header)

        if not print_only:
            diagnostic_output.append(diag_header + "\n")

        cur_output = None
        with io.StringIO() as buf, redirect_stdout(buf), redirect_stderr(buf):
            try:
                if callable(cmd):
                    cmd_output = cmd()
                    # Some methods return a str output, not printing to stdout
                    if cmd_output is not None:
                        print(cmd_output)
                else:
                    run_single_command(cmd)
                cur_output = buf.getvalue()
            except Exception as e:
                print(f"Error encountered during ({display_name}): {e}")

        if cur_output:
            if print_only:
                print(cur_output)
            else:
                diagnostic_output.append(cur_output + "\n")

    # Should we provide default UUID as the Tutorial MEP?  It would
    # likely slow down the diagnostic by 10-20 seconds while starting the UEP
    # if ep_uuid is None:
    #     ep_uuid = TUTORIAL_EP_UUID

    if ep_uuid:
        ep_test_title = f"Run sample function on endpoint {ep_uuid} via Executor"
        if print_only or verbose:
            print_highlight(f"== Diagnostic: {ep_test_title} ==")
        with io.StringIO() as buf, redirect_stdout(buf), redirect_stderr(buf):
            with Executor(client=gcc, endpoint_id=ep_uuid) as gce:
                # sdk_tutorial_sample_simple_function is just a Hello World
                # Optionally use sdk_tutorial_sample_function which has local imports
                fut = gce.submit(sdk_tutorial_sample_simple_function)
                try:
                    # result = "fake"
                    result = fut.result(timeout=DIAG_TIMEOUT_S)
                    print(f"Endpoint simple function execution result:\n  {result}")
                except TimeoutError:
                    print("--> Timed out sending task, endpoint may be offline")

                # Need to manually shutdown or the Executor auto shutdown blocks
                gce.shutdown(wait=False, cancel_futures=False)
                # This feels icky, should we use Client and polling instead?
                while _RESULT_WATCHERS:
                    rw_item = next(iter(_RESULT_WATCHERS.values()))
                    rw_item.shutdown(wait=False, cancel_futures=True)
                cur_output = buf.getvalue()
        if cur_output:
            if print_only:
                print(cur_output)
            else:
                diagnostic_output.append(cur_output + "\n")

    if not print_only:
        # Creates a zipped version for easier transport back to Globus
        with gzip.open(zip_filename, "wb") as f_out:
            out_bytes = "".join(diagnostic_output).encode()
            f_out.write(out_bytes)

        print(f"Compressed diagnostic output successfully written to {zip_filename}")


def do_diagnostic_base(diagnostic_args):
    """
    This is the root diagnostic method that parses user arguments and then calls
    the wrapper that in turn executes the individual diagnostic methods
    """

    # just_fix... supposedly is needed to make sure windows console colors work,
    #   this line is a no-op on other platforms
    colorama.just_fix_windows_console()

    DIAGNOSTIC_HELP_TEXT = (
        "This utility gathers local hardware specifications,  tests "
        "connectivity to the Globus Compute web services, and collates "
        "portions of local Compute Endpoint log files, if present, to a "
        "local compressed file."
    )

    parser = argparse.ArgumentParser(
        description="Run diagnostics for Globus Compute",
        epilog=DIAGNOSTIC_HELP_TEXT,
    )
    parser.add_argument(
        "-p",
        "--print-only",
        action="store_true",
        default=False,
        help=(
            "Do not generate a Gzip-compressed file. Print diagnostic results "
            "to the console instead."
        ),
    )
    parser.add_argument(
        "-k",
        "--log-kb",
        metavar="number",
        action="store",
        default=1024,
        type=int,
        help=(
            "Specify the number of kilobytes (KB) to read from log files."
            " Defaults to 1024 KB (1 MB) per file."
        ),
    )
    parser.add_argument(
        "-v",
        "--verbose",
        default=False,
        action="store_true",
        help=(
            "When writing diagnostic output to local compressed file, also "
            "print the name of each test to stdout as they are being run, "
            "to help monitor diagnostic progress."
        ),
    )
    parser.add_argument(
        "-e",
        "--endpoint-uuid",
        default=None,
        type=str,
        help=(
            "Test an endpoint by registering a sample function and sending "
            "a task to it using the newly registered function.  An endpoint "
            "UUID is required."
        ),
    )

    args = parser.parse_args(diagnostic_args)
    run_all_diags_wrapper(
        args.print_only, args.log_kb * 1024, args.verbose, args.endpoint_uuid
    )


def do_diagnostic():
    """
    Just a wrapper for easier testing
    """
    return do_diagnostic_base(sys.argv[1:])


if __name__ == "__main__":
    do_diagnostic()
