from __future__ import annotations

import logging
import os
import platform
import shlex
import shutil
import subprocess
import sys

from globus_compute_sdk.sdk.utils import display_name, simple_humanize


def _format_psmem(psdata) -> str:
    data = psdata._asdict()
    max_name_len = max(map(len, data.keys()))
    max_val_len = max(map(len, map(str, data.values())))
    lines = []
    for fname, fval in data.items():
        fname = fname.capitalize()
        if fname == "Percent":
            val = f"{fval:>6} % (used)"
        else:
            v, u = simple_humanize(fval)
            val = f"{v:>6} {u + 'B':<3} ({fval:>{max_val_len}})"
        lines.append(f"{fname:>{max_name_len}}: {val}")

    return "\n".join(lines)


@display_name("psutil.virtual_memory()")
def mem_info() -> str:
    import psutil  # import here bc psutil is installed with endpoint but not sdk

    return _format_psmem(psutil.virtual_memory())


@display_name("psutil.swap_memory()")
def swap_info() -> str:
    import psutil  # import here bc psutil is installed with endpoint but not sdk

    return _format_psmem(psutil.swap_memory())


@display_name("python_runtime_host_info")
def python_runtime_host_info() -> str:
    return (
        f"          Version: {sys.version}"
        f"\n     Version info: {sys.version_info}"
        f"\n Interpreter path: {sys.executable}"
        "\n"
        f"\n CPU architecture: {platform.processor()}"
        f"\n   CPU core count: {os.cpu_count()}"
        "\n"
        f"\n        Node name: {platform.node()}"
        f"\n         Platform: {platform.platform()}"
        "\n"
        f"\n             HOME: {os.getenv('HOME')}"
        f"\n             USER: {os.getenv('USER')}"
        f"\n         USERNAME: {os.getenv('USERNAME')}"
        f"\n             LANG: {os.getenv('LANG')}"
        f"\n  XDG_RUNTIME_DIR: {os.getenv('XDG_RUNTIME_DIR')}"
        f"\n             PATH: {os.getenv('PATH')}"
        f"\n      VIRTUAL_ENV: {os.getenv('VIRTUAL_ENV')}"
        "\n"
        f"\n              uid: {os.getuid()}"
    )


def cpu_info() -> str | None:
    if sys.platform == "linux":
        return "lscpu"
    elif sys.platform == "darwin":
        return "sysctl -a | grep machdep.cpu"
    elif sys.platform == "win32":
        return "wmic CPU get NAME"
    else:
        return None


def _run_command(cmd: str) -> str | None:
    logger = logging.getLogger(__name__)  # get logger at call site (ie, endpoint)

    cmd_list = shlex.split(cmd)
    arg0 = cmd_list[0]

    if not shutil.which(arg0):
        logger.info(f"{arg0} was not found in the PATH")
        return None

    try:
        res = subprocess.run(cmd_list, timeout=30, capture_output=True, text=True)
        if res.stdout:
            return str(res.stdout)
        if res.stderr:
            return str(res.stderr)
        return "Warning: command had no output on stdout or stderr"
    except subprocess.TimeoutExpired:
        logger.exception(f"Command `{cmd}` timed out")
        return (
            "Error: command timed out (took more than 30 seconds). "
            "See endpoint logs for more details."
        )
    except Exception as e:
        logger.exception(f"Command `{cmd}` failed")
        return (
            f"An error of type {type(e).__name__} occurred. "
            "See endpoint logs for more details."
        )


def hardware_commands_list() -> list:
    """
    Commands that can also be useful in globus-compute-diagnostic
    """
    return [
        python_runtime_host_info,
        mem_info,
        swap_info,
        cpu_info(),
        "nvidia-smi",
        "df -h",
    ]


def run_hardware_report(env: str | None = None) -> str:
    """
    :param env:    # The environment this is to be run on, will
                     be highlighted in the output headers
                     for example == Compute Endpoint Worker == ... ==
                     This can also be run on the login node/Client
    :returns:      A line break separated output of all the
                     individual reports
    """
    commands = hardware_commands_list()

    # Separate out some commands that are duplicated in general
    # SDK diagnostics or that we only want to run here
    commands.extend(
        [
            "lshw -C display",
            "globus-compute-endpoint version",
        ]
    )

    outputs = []
    for cmd in commands:
        if callable(cmd):
            display_name = getattr(
                cmd, "display_name", f"{cmd.__module__}.{cmd.__name__}()"
            )
            display_name = f"python: {display_name}"
            output = cmd()
        else:
            display_name = f"shell: {cmd}"
            output = _run_command(cmd)

        if output:
            env_str = f"== {env} == " if env else ""
            outputs.append(f"{env_str}== {display_name} ==\n{output}")

    return "\n\n".join(outputs)
