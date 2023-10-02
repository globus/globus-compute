from __future__ import annotations

import glob
import os
import shlex
import shutil
import socket
import ssl
import subprocess
import sys
from urllib.parse import urlparse

import click
from globus_compute_sdk.sdk._environments import (
    get_amqp_service_host,
    get_web_service_url,
)
from globus_compute_sdk.sdk.web_client import WebClient


def cat(path: str, wildcard: bool = False, max_bytes: int = 0):
    max_bytes = max(0, max_bytes)

    full_path = os.path.expanduser(path)
    if wildcard:
        files = glob.glob(full_path)
    else:
        files = [full_path]

    def kernel() -> None:
        for filename in files:
            cat_cmd = "cat " + filename
            hline = "=" * len(cat_cmd)
            click.echo(cat_cmd + "\n" + hline)
            if os.path.exists(filename):
                with open(filename, "rb") as f:
                    if max_bytes:
                        file_size = os.fstat(f.fileno()).st_size
                        f.seek(max(file_size - max_bytes, 0))
                    content = f.read().replace(b"\n", b"\n | ")
                click.echo(b" | " + content)
            else:
                click.secho(f"No file named {filename}\n", fg="red", bold=True)
            hline = "-" * len(cat_cmd)
            click.echo(hline + "\n")

    kernel.display_name = f"func:cat({path})"  # type: ignore
    return kernel


def test_conn(host: str, port: int, timeout: int = 5):
    def kernel():
        try:
            socket.create_connection((host, port), timeout=timeout)
            click.echo(f"Connected successfully to {host} over port {port}!\n")
        except OSError as e:
            click.secho(
                f"Connection failed to {host} over port {port}: {e}\n",
                fg="red",
                bold=True,
            )

    kernel.display_name = f"func:test_conn({host}, {port})"  # type: ignore
    return kernel


def test_ssl_conn(host: str, port: int, timeout: int = 5):
    def kernel() -> None:
        context = ssl.create_default_context()
        try:
            with socket.create_connection((host, port), timeout=timeout) as sock:
                with context.wrap_socket(sock, server_hostname=host) as ssock:
                    conn_data = ssock.cipher()
                    cipher, version, _ = conn_data if conn_data else (None, None, None)
            click.echo(
                f"Successfully established SSL connection with {host}:{port}!\n"
                f"Version: {version}\n"
                f"Cipher:  {cipher}\n"
            )
        except OSError as e:
            click.secho(
                f"Failed to establish SSL connection with {host}:{port} - {e}\n",
                fg="red",
                bold=True,
            )

    kernel.display_name = f"func:test_ssl_conn({host}, {port})"  # type: ignore
    return kernel


def get_service_versions(base_url: str):
    def kernel():
        wc = WebClient(base_url=base_url)
        res = wc.get_version(service="all")
        click.echo(f"{res}\n")

    kernel.display_name = f"func:get_service_versions({base_url})"  # type: ignore
    return kernel


def get_openssl_version():
    click.echo(f"{ssl.OPENSSL_VERSION}\n")


def get_python_version():
    click.echo(f"Python version {sys.version}\n")


def which_python():
    click.echo(f"{sys.executable}\n")


def _run_command(cmd: str):
    cmd_list = shlex.split(cmd)
    arg0 = cmd_list[0]

    if not shutil.which(arg0):
        click.secho(f"{arg0} was not found in the PATH\n", fg="red", bold=True)
        return

    try:
        res = subprocess.run(cmd_list, timeout=30, capture_output=True)
        if res.stdout:
            click.echo(res.stdout)
        if res.stderr:
            click.secho(res.stderr.decode("utf-8"), fg="red", bold=True)
    except subprocess.TimeoutExpired:
        click.secho("Command timed out\n", fg="red", bold=True)
    except Exception as e:
        click.secho(f"Command failed: {e}\n", fg="red", bold=True)


def run_self_diagnostic(log_bytes: int = 0):
    web_svc_url = get_web_service_url()
    web_svc_host = urlparse(web_svc_url).netloc
    amqp_svc_host = get_amqp_service_host()

    commands = [
        "uname -a",
        cat("/etc/os-release"),
        "whoami",
        which_python,
        get_python_version,
        "pip freeze",
        get_openssl_version,
        test_conn(web_svc_host, 443),
        test_conn(amqp_svc_host, 5671),
        test_ssl_conn(web_svc_host, 443),
        test_ssl_conn(amqp_svc_host, 5671),
        get_service_versions(web_svc_url),
        "ip addr",
        "ifconfig",
        "ip route",
        "netstat -r",
        "globus-compute-endpoint whoami",
        "globus-compute-endpoint list",
        cat("~/.globus_compute/*/config.*", wildcard=True),
        cat("~/.globus_compute/*/endpoint.log", wildcard=True, max_bytes=log_bytes),
    ]

    for cmd in commands:
        display_name = (
            str(cmd)
            if not callable(cmd)
            else getattr(cmd, "display_name", f"func:{cmd.__name__}()")
        )
        click.secho(f"== Diagnostic: {display_name} ==", fg="yellow", bold=True)

        if callable(cmd):
            cmd()
            continue

        _run_command(cmd)
