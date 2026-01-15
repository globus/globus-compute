import os
import pathlib
from unittest import mock

import pytest
from click import ClickException
from globus_compute_endpoint.boot_persistence import (
    _systemd_service_name,
    disable_on_boot,
    enable_on_boot,
)
from globus_compute_endpoint.endpoint.endpoint import Endpoint
from globus_sdk import UserApp
from pyfakefs import fake_filesystem as fakefs

_MOCK_BASE = "globus_compute_endpoint.boot_persistence."


@pytest.fixture
def ep_name(randomstring):
    return randomstring()


@pytest.fixture
def fake_ep_dir(fs: fakefs.FakeFilesystem, ep_name) -> pathlib.Path:
    ep_dir = pathlib.Path.home() / ".globus_compute" / ep_name
    fs.create_dir(ep_dir)
    ep_config = Endpoint._config_file_path(ep_dir)
    ep_config.write_text(
        """
display_name: null
engine:
    type: ThreadPoolEngine
        """.strip()
    )
    return ep_dir


@pytest.fixture
def systemd_unit_dir(fs: fakefs.FakeFilesystem, mocker):
    unit_dir = pathlib.Path("/etc/systemd/system")
    mocker.patch(f"{_MOCK_BASE}_SYSTEMD_UNIT_DIR", unit_dir)
    fs.create_dir(unit_dir)

    return unit_dir


def test_enable_on_boot_happy_path(fake_ep_dir, systemd_unit_dir, ep_name, capsys):
    mock_app = mock.Mock(spec=UserApp)

    enable_on_boot(fake_ep_dir, mock_app)

    mock_app.login.assert_called_once()

    assert any(ep_name in f for f in os.listdir(systemd_unit_dir))

    out = capsys.readouterr().out
    assert "Systemd service installed" in out
    assert "systemctl" in out
    assert ep_name in out


def test_enable_on_boot_no_systemd(fake_ep_dir, mocker):
    mocker.patch(f"{_MOCK_BASE}_systemd_available", return_value=False)
    mock_app = mock.Mock(spec=UserApp)

    with pytest.raises(ClickException) as e:
        enable_on_boot(fake_ep_dir, mock_app)

    assert "Systemd not found" in str(e)


@pytest.mark.parametrize("exists", [True, False])
def test_enable_on_boot_active_ep(fake_ep_dir, systemd_unit_dir, mocker, exists):
    mock_app = mock.Mock(spec=UserApp)
    mocker.patch(
        f"{_MOCK_BASE}Endpoint.check_pidfile",
        return_value={"exists": exists, "active": True},
    )

    with pytest.raises(ClickException) as e:
        enable_on_boot(fake_ep_dir, mock_app)

    assert "currently running" in str(e)


def test_enable_on_boot_service_file_already_exists(
    fake_ep_dir, systemd_unit_dir, ep_name, randomstring, mocker
):
    mock_app = mock.Mock(spec=UserApp)
    unit = systemd_unit_dir / f"{_systemd_service_name(ep_name)}.service"
    unit.write_text(randomstring())

    with pytest.raises(ClickException) as e:
        enable_on_boot(fake_ep_dir, mock_app)

    assert "already exists" in str(e)
    assert ep_name in str(e)


def test_enable_on_boot_no_perms(
    fake_ep_dir, systemd_unit_dir, fs: fakefs.FakeFilesystem, mocker
):
    mock_app = mock.Mock(spec=UserApp)
    systemd_unit_dir.chmod(fakefs.PERM_READ)

    with pytest.raises(ClickException) as pyt_exc:
        enable_on_boot(fake_ep_dir, mock_app)

    assert "Unable to create systemd unit file" in str(pyt_exc.value)


def test_disable_on_boot_happy_path(
    fake_ep_dir, systemd_unit_dir, fs: fakefs.FakeFilesystem, capsys, ep_name
):
    fs.create_file(systemd_unit_dir / f"{_systemd_service_name(ep_name)}.service")

    disable_on_boot(fake_ep_dir)

    out = capsys.readouterr().out
    assert "Run the following to disable on-boot-persistence" in out
    assert "systemctl" in out
    assert ep_name in out


def test_disable_on_boot_no_systemd(fake_ep_dir, mocker):
    mocker.patch(f"{_MOCK_BASE}_systemd_available", return_value=False)

    with pytest.raises(ClickException) as e:
        disable_on_boot(fake_ep_dir)

    assert "Systemd not found" in str(e)


def test_disable_on_boot_checks_for_existing_unit_file(
    fake_ep_dir, systemd_unit_dir, fs: fakefs.FakeFilesystem
):
    service_path = systemd_unit_dir / f"{_systemd_service_name(ep_name)}.service"
    if fs.exists(service_path):
        fs.remove(service_path)

    with pytest.raises(ClickException) as e:
        disable_on_boot(fake_ep_dir)

    assert "existing systemd unit" in str(e)
