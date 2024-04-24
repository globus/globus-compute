import getpass
import os
import pathlib
import shutil
import textwrap

from click import ClickException
from globus_compute_endpoint.endpoint.config.utils import get_config
from globus_compute_endpoint.endpoint.endpoint import Endpoint
from globus_compute_sdk.sdk.login_manager import LoginManager

_SYSTEMD_UNIT_DIR = pathlib.Path("/etc/systemd/system")

_SYSTEMD_UNIT_TEMPLATE = """[Unit]
Description=systemd service for Globus Compute Endpoint "{ep_name}"
After=network.target
StartLimitIntervalSec=0

[Service]
ExecStart={gce_exe} start {ep_name}
User={user_account}
Type=simple
Restart=always
RestartSec=1

[Install]
WantedBy=multi-user.target
"""

_DISABLE_SYSTEMD_COMMANDS = """systemctl stop {service_name}
systemctl disable {service_name}
rm {unit_file_path}"""


def _systemd_service_name(ep_name: str) -> str:
    return f"globus-compute-endpoint-{ep_name}"


def _systemd_available() -> bool:
    return _SYSTEMD_UNIT_DIR.exists()


def enable_on_boot(ep_dir: pathlib.Path):
    if not _systemd_available():
        raise ClickException(
            "Systemd not found. On-boot persistence is currently only implemented for"
            " systemd; check this machine's init system documentation for alternatives."
        )

    ep_name = ep_dir.name

    config = get_config(ep_dir)
    if config.detach_endpoint:
        # config.py takes priority if it exists
        if os.path.isfile(ep_dir / "config.py"):
            # can't give users a nice packaged command to run here, so just tell them
            msg = (
                f"Persistent endpoints cannot run in detached mode. Update {ep_name}'s"
                " config to set detach_endpoint=False and try again."
            )
        else:
            # _can_ give a nice packaged command if they use yaml, though
            msg = (
                "Persistent endpoints cannot run in detached mode. Run the following"
                f" command to update {ep_name}'s config:"
                f"\n\techo 'detach_endpoint: false' >> {ep_dir / 'config.yaml'}"
            )
        raise ClickException(msg)

    if Endpoint.check_pidfile(ep_dir)["active"]:
        raise ClickException(
            "Cannot enable on-boot persistence for an endpoint that is currently"
            f" running. Stop endpoint {ep_name} and try again."
        )

    # ensure that credentials exist when systemd tries to start endpoint, so that
    # auth login flow doesn't run under systemd
    LoginManager().ensure_logged_in()

    service_name = _systemd_service_name(ep_name)
    unit_file_path = _SYSTEMD_UNIT_DIR / f"{service_name}.service"
    unit_file_text = _SYSTEMD_UNIT_TEMPLATE.format(
        ep_name=ep_name,
        gce_exe=shutil.which("globus-compute-endpoint"),
        user_account=getpass.getuser(),
    )

    try:
        with open(unit_file_path, "x") as unit_file:
            unit_file.write(unit_file_text)
    except FileExistsError:
        raise ClickException(
            "Cannot create systemd unit file -"
            f" a file with the name {unit_file_path} already exists"
        )
    except PermissionError as e:
        raise ClickException(f"{e}\n\nUnable to create unit file. Are you root?")

    print(
        "Systemd service installed. Run"
        f"\n\tsudo systemctl enable {service_name} --now"
        "\nto enable the service and start the endpoint."
    )


def disable_on_boot(ep_dir: pathlib.Path):
    if not _systemd_available():
        raise ClickException(
            "Systemd not found. On-boot persistence is currently only implemented for"
            " systemd; check this machine's init system documentation for alternatives."
        )

    ep_name = ep_dir.name
    service_name = _systemd_service_name(ep_name)
    unit_file_path = _SYSTEMD_UNIT_DIR / f"{service_name}.service"

    if not unit_file_path.exists():
        raise ClickException(
            "There does not appear to be an existing systemd unit for endpoint"
            f" {ep_name} (unit file {unit_file_path} not found)"
        )

    commands = textwrap.indent(
        _DISABLE_SYSTEMD_COMMANDS.format(
            service_name=service_name, unit_file_path=unit_file_path
        ),
        "\t",
    )
    print(f"Run the following to disable on-boot-persistence:\n{commands}")
