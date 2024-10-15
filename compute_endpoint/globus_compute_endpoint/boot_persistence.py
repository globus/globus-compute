import getpass
import pathlib
import shutil
import textwrap

from click import ClickException
from globus_compute_endpoint.endpoint.config.utils import get_config
from globus_compute_endpoint.endpoint.endpoint import Endpoint
from globus_sdk import GlobusApp

_SYSTEMD_UNIT_DIR = pathlib.Path("/etc/systemd/system")

_SYSTEMD_UNIT_TEMPLATE = """[Unit]
Description=Globus Compute Endpoint "{ep_name}"
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


def enable_on_boot(ep_dir: pathlib.Path, app: GlobusApp):
    if not _systemd_available():
        raise ClickException(
            "Systemd not found. On-boot persistence is currently only implemented for"
            " systemd; check this machine's init system documentation for alternatives."
        )

    ep_name = ep_dir.name

    config = get_config(ep_dir)
    if not config.multi_user:
        if config.detach_endpoint:
            # config.py takes priority if it exists
            if (ep_dir / "config.py").is_file():
                # can't give users a nice packaged command to run here; just tell them
                msg = (
                    "Persistent endpoints cannot run in detached mode.  Update"
                    f" {ep_name} config to set `detach_endpoint=False` and try again."
                )
            else:
                # _can_ give a nice packaged command if they use yaml, though
                msg = (
                    "Persistent endpoints cannot run in detached mode.  Run the"
                    f" following command to update {ep_name}'s config:"
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
    app.login()

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
        raise ClickException(
            f"{e}\n\nUnable to create systemd unit file.  (Do you need to be root?)\n"
            f"\n----- Systemd unit file content -----\n{unit_file_text}"
        )

    print(
        f"Systemd service installed at {unit_file_path}. Run"
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
