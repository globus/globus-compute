from __future__ import annotations

import argparse
import contextlib
import glob
import gzip
import os
import platform
import shlex
import shutil
import socket
import ssl
import subprocess
import sys
from datetime import datetime as dt
from urllib.parse import urlparse

import colorama
import rich
from globus_compute_sdk.sdk._environments import TUTORIAL_EP_UUID  # noqa TODO
from globus_compute_sdk.sdk._environments import (
    ensure_compute_dir,
    get_amqp_service_host,
    get_web_service_url,
)
from globus_compute_sdk.sdk.hardware_report import hardware_commands_list
from globus_compute_sdk.sdk.utils import display_name
from globus_compute_sdk.sdk.utils.tutorial_endpoint import TutorialEndpointTester
from globus_compute_sdk.sdk.web_client import WebClient

OUTPUT_FILENAME = "globus_compute_diagnostic_{}.txt"
CUR_CMD_OUTPUT_FILENAME = "globus_compute_diagnostic_current_test.txt"


def cat(path: str, wildcard: bool = False, max_bytes: int = 0):
    max_bytes = max(0, max_bytes)

    full_path = os.path.expanduser(path)
    if wildcard:
        files = glob.glob(full_path, recursive=True)
    else:
        files = [full_path]

    @display_name(f"cat({path})")
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
    def kernel():
        try:
            socket.create_connection((host, port), timeout=timeout)
            print(f"Connected successfully to {host} over port {port}!\n")
        except OSError as e:
            print_warning(f"Connection failed to {host} over port {port}: {e}\n")

    kernel.display_name = f"test_conn({host}, {port})"  # type: ignore
    return kernel


def test_ssl_conn(host: str, port: int, timeout: int = 5):
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
    def kernel():
        # Just adds a newline to the version info for better formatting
        version_info = WebClient(base_url=base_url).get_version(service="all")
        print(f"{version_info}\n")

    kernel.display_name = f"get_service_versions({base_url})"  # type: ignore
    return kernel


def ifconfig_wrapper():
    if sys.platform in ["win32"]:
        return "ipconfig"

    return "ifconfig"


@display_name("OpenSSL version")
def print_openssl_version():
    print(f"{ssl.OPENSSL_VERSION}\n")


def print_warning(s: str):
    """"""
    # Prints both to stderr (goes to console) and to stdout (captured into file)
    rich.print(f"[bold red]{s}[/bold red]", file=sys.stderr)
    print(s)


def print_highlight(s: str):
    rich.print(f"[bold yellow]{s}[/bold yellow]")


def os_info_cmd():
    if sys.platform == "linux":
        return cat("/etc/os-release")
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
        }

        cmd_base = cmd.split()[0]
        if cmd_base in BY_PLATFORM:
            return cmd if sys.platform in BY_PLATFORM.get(cmd_base) else None
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
    run_attempt = subprocess.run("globus-compute-endpoint", capture_output=True)
    if bool(run_attempt.stderr) and "command not found" in run_attempt.stderr.decode():
        return "globus-compute-endpoint is not installed"
    else:
        # `which` by itself may return a value even if gce is not installed.
        # It will return the shim in pyenv, which contains info for other PY versions
        ep_path = shutil.which("globus-compute-endpoint")
        if ep_path:
            return f"globus-compute-endpoint is installed at {ep_path}"
        else:
            return "globus-compute-endpoint is not installed"


def general_commands_list():
    """
    General diagnostic commands
    Excludes endpoint hardware reports and sending tasks to endpoints
    """
    web_svc_url = get_web_service_url()
    web_svc_host = urlparse(web_svc_url).netloc
    amqp_svc_host = get_amqp_service_host()

    return [
        "uname -a",
        os_info_cmd(),
        "whoami",
        "pip freeze",
        print_openssl_version,
        platform.processor,
        test_conn(web_svc_host, 443),
        test_conn(amqp_svc_host, 5671),
        test_ssl_conn(web_svc_host, 443),
        test_ssl_conn(amqp_svc_host, 5671),
        print_service_versions(web_svc_url),
        ifconfig_wrapper(),
        if_supported_on_os("ip addr"),
        if_supported_on_os("ip route"),
        # "netstat -r",   #  May be slow on macOS TODO test on windows
    ]


def get_diagnostic_commands(log_bytes: int):

    commands = filter_by_os(hardware_commands_list())
    commands.extend(general_commands_list())

    gc_home_dir = ensure_compute_dir()

    # Run endpoint related diagnostics when it is installed
    ep_install_dir = print_endpoint_install_dir()
    if "is not installed" not in ep_install_dir:
        commands.extend([
                print(ep_install_dir),
                "globus-compute-endpoint whoami",
                "globus-compute-endpoint list",
        ])

        if log_bytes > 0:
            # Specifying 0 means we don't want to print logs.
            # TODO maybe we want to disallow 0 for real world use (testing use)
            commands.extend([
                    cat(f"{gc_home_dir}/*/*.yaml", wildcard=True, max_bytes=log_bytes),
                    cat(f"{gc_home_dir}/*/*.log", wildcard=True, max_bytes=log_bytes),
                    cat(f"{gc_home_dir}/*/*.py", wildcard=True, max_bytes=log_bytes),
                    cat(f"{gc_home_dir}/*/*.j2", wildcard=True, max_bytes=log_bytes),
                    cat(f"{gc_home_dir}/*/*.json", wildcard=True, max_bytes=log_bytes),
                ]
            )
    else:
        ep_install_dir = None

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
            res = subprocess.run(cmd_list, timeout=15, capture_output=True, text=True)
        if res.stdout:
            print(res.stdout)
        if res.stderr:
            print_warning(res.stderr)
    except subprocess.TimeoutExpired:
        print_warning("Command timed out\n")
    except Exception as e:
        print_warning(f"Command failed: {e}\n")


def append_diagnostic_result(
    heading, summary_filename, heading_only: bool = False, console: bool = False
):
    with open(summary_filename, "a") as log_summary:
        if heading:
            log_summary.write(f"\n== Diagnostic: {heading} ==\n")
        if heading_only:
            print()
        else:
            with open(CUR_CMD_OUTPUT_FILENAME) as f:
                output = f.read()
                log_summary.write(output)
                if console:
                    print(output)

            # Cleanup to save space - users can unzip to view if necessary
            os.remove(CUR_CMD_OUTPUT_FILENAME)


def run_all_commands_wrapper(
    print_to_console: bool, log_bytes: int, ep_uuid: str | None = None
):
    """
    This is the diagnostic wrapper that obtains the list of commands to be run
    (which depends on whether we are in the SDK or Endpoint, and filters based
    on current platform OS), then runs the individual diagnostic methods one by
    one, and collates them into a zip file upon completion.

    For each diagnostic, this method prints what is being run to stdout, then
    executes the diagnostic itself which may also print to stdout if the flag
    is specified.  All output is added to a file as tests are run, and the
    output file is compressed at the conclusion of the method.

    :param print_to_console:  Whether to print the diagnostic output to console
                                as they are being run.  If False, will only
                                print the name of each step as they are run
    :param log_bytes:         # of bytes to limit the individual endpoint log
                                extractions to
    :param ep_uuid:           # If specified, register a sample function and
                                send a task that uses the function UUID to the
                                endpoint
    """

    current_date = dt.now().strftime("%Y-%m-%d")
    log_filename = OUTPUT_FILENAME.format(current_date)
    zip_filename = f"{log_filename}.gz"

    # Initialize empty file
    open(log_filename, "w").close()

    diag_cmds, ep_install_dir = get_diagnostic_commands(log_bytes)
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
        print_highlight(f"== Diagnostic: {display_name} ==")

        append_diagnostic_result(display_name, log_filename, True)
        with open(CUR_CMD_OUTPUT_FILENAME, "w") as f:
            with contextlib.redirect_stdout(f):
                try:
                    if callable(cmd):
                        cmd_output = cmd()
                        # Some methods return a str output, not printing to stdout
                        if cmd_output is not None:
                            print(cmd_output)
                    else:
                        run_single_command(cmd)
                except Exception as e:
                    print(f"Error encountered during ({display_name}): {e}")

                # cmd() if callable(cmd) else run_single_command(cmd)

        append_diagnostic_result(None, log_filename, console=print_to_console)

    # TODO Remove default UUID of the Tutorial MEP"
    # if ep_uuid is None:
    #     ep_uuid = TUTORIAL_EP_UUID

    if ep_uuid:
        ep_tester = TutorialEndpointTester(ep_uuid)
        ep_test_title = "Run sample function on Tutorial Endpoint via Executor"
        print_highlight(f"== Diagnostic: {ep_test_title} ==")
        with open(CUR_CMD_OUTPUT_FILENAME, "w") as f:
            with contextlib.redirect_stdout(f):
                ep_tester.run_new_func_with_executor()

        append_diagnostic_result(ep_test_title, log_filename, console=print_to_console)

    # Creates a zipped version for easier transport back to Globus
    with open(log_filename, "rb") as f_in, gzip.open(zip_filename, "wb") as f_out:
        f_out.writelines(f_in)
    os.remove(log_filename)


def do_diagnostic_base(diagnostic_args):
    """
    This is the root diagnostic method that parses user arguments and then calls
    the wrapper that in turn executes the individual diagnostic methods
    """

    # Supposedly is needed to make sure windows console colors work, is a no-op
    #   on other platforms
    colorama.just_fix_windows_console()

    parser = argparse.ArgumentParser(description="Run diagnostics for Globus Compute")
    parser.add_argument(
        "-p",
        "--print",
        action="store_true",
        default=False,
        help=(
            "print the diagnostic output to console, in addition to saving it to "
            "a local Gzip-compressed file."
        ),
    )
    parser.add_argument(
        "-kb",
        "--log-kb",
        metavar="number",
        action="store",
        # default=5120,
        default=10,   # TODO remove this and use the 5120 before merge
        type=int,
        help=(
            "Specify the number of kilobytes (KB) to read from log files."
            " Defaults to 5,120 KB (5 MB)."
        ),
    )
    parser.add_argument(
        "-ep",
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
    run_all_commands_wrapper(args.print, args.log_kb * 1024, args.endpoint_uuid)


def do_diagnostic():
    """
    Just a wrapper for easier testing
    """
    return do_diagnostic_base(sys.argv[1:])


if __name__ == "__main__":
    do_diagnostic()
