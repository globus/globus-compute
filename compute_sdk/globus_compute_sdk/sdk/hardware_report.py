from __future__ import annotations

import logging
import os
import platform
import shlex
import shutil
import subprocess
import sys


def display_name(name: str):
    def decorator(func):
        func.display_name = name
        return func

    return decorator


@display_name("psutil.virtual_memory()")
def mem_info() -> str:
    import psutil  # import here bc psutil is installed with endpoint but not sdk

    svmem = psutil.virtual_memory()
    return "\n".join(f"{k}: {v}" for k, v in svmem._asdict().items())


@display_name("sys.version")
def python_version() -> str:
    return str(sys.version)


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


def run_hardware_report() -> str:
    commands = [
        platform.processor,
        os.cpu_count,
        "lscpu",
        "lshw -C display",
        "nvidia-smi",
        mem_info,
        "df",
        platform.node,
        platform.platform,
        python_version,
        "globus-compute-endpoint version",
    ]

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
            outputs.append(f"== {display_name} ==\n{output}")

    return "\n\n".join(outputs)
