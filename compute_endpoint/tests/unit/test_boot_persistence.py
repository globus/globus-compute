import os
import pathlib

import pytest
import yaml
from click import ClickException
from globus_compute_endpoint.boot_persistence import (
    _systemd_service_name,
    disable_on_boot,
    enable_on_boot,
)
from globus_compute_endpoint.endpoint.endpoint import Endpoint
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
detach_endpoint: false
display_name: null
engine:
    type: GlobusComputeEngine
    provider:
        type: LocalProvider
        init_blocks: 1
        min_blocks: 0
        max_blocks: 1
        """.strip()
    )
    return ep_dir


@pytest.fixture
def systemd_unit_dir(fs: fakefs.FakeFilesystem, mocker):
    unit_dir = pathlib.Path("/etc/systemd/system")
    mocker.patch(f"{_MOCK_BASE}_SYSTEMD_UNIT_DIR", unit_dir)
    fs.create_dir(unit_dir)

    return unit_dir


def test_enable_on_boot_happy_path(
    fake_ep_dir, systemd_unit_dir, ep_name, mocker, capsys
):
    mock_login_manager = mocker.patch(f"{_MOCK_BASE}LoginManager")
    mock_ensure_logged_in = mock_login_manager.return_value.ensure_logged_in

    enable_on_boot(fake_ep_dir)

    mock_ensure_logged_in.assert_called_once()

    assert any(ep_name in f for f in os.listdir(systemd_unit_dir))

    out = capsys.readouterr().out
    assert "Systemd service installed" in out
    assert "systemctl" in out
    assert ep_name in out


def test_enable_on_boot_no_systemd(fake_ep_dir, mocker):
    mocker.patch(f"{_MOCK_BASE}_systemd_available", return_value=False)

    with pytest.raises(ClickException) as e:
        enable_on_boot(fake_ep_dir)

    assert "Systemd not found" in str(e)


@pytest.mark.parametrize("mu", (True, False))
def test_enable_on_boot_detach_endpoint(
    mocker, fake_ep_dir, systemd_unit_dir, mu: bool, fs: fakefs.FakeFilesystem
):
    cfg_path = fake_ep_dir / "config.yaml"
    cfg = yaml.safe_load(cfg_path.read_text())
    del cfg["detach_endpoint"]
    if mu:
        cfg["multi_user"] = True
        del cfg["engine"]
    cfg_path.write_text(yaml.dump(cfg))

    if mu:
        mocker.patch(f"{_MOCK_BASE}LoginManager")
        mock_print = mocker.patch(f"{_MOCK_BASE}print")
        enable_on_boot(fake_ep_dir)
        assert "Systemd service installed at " in mock_print.call_args[0][0]
    else:
        with pytest.raises(ClickException) as e:
            enable_on_boot(fake_ep_dir)

        assert "cannot run in detached mode" in str(e)


@pytest.mark.parametrize("exists", [True, False])
def test_enable_on_boot_active_ep(fake_ep_dir, systemd_unit_dir, mocker, exists):
    mocker.patch(f"{_MOCK_BASE}LoginManager")
    mocker.patch(
        f"{_MOCK_BASE}Endpoint.check_pidfile",
        return_value={"exists": exists, "active": True},
    )

    with pytest.raises(ClickException) as e:
        enable_on_boot(fake_ep_dir)

    assert "currently running" in str(e)


def test_enable_on_boot_service_file_already_exists(
    fake_ep_dir, systemd_unit_dir, ep_name, randomstring, mocker
):
    mocker.patch(f"{_MOCK_BASE}LoginManager")
    unit = systemd_unit_dir / f"{_systemd_service_name(ep_name)}.service"
    unit.write_text(randomstring())

    with pytest.raises(ClickException) as e:
        enable_on_boot(fake_ep_dir)

    assert "already exists" in str(e)
    assert ep_name in str(e)


def test_enable_on_boot_no_perms(
    fake_ep_dir, systemd_unit_dir, fs: fakefs.FakeFilesystem, mocker
):
    mocker.patch(f"{_MOCK_BASE}LoginManager")
    systemd_unit_dir.chmod(fakefs.PERM_READ)

    with pytest.raises(ClickException) as pyt_exc:
        enable_on_boot(fake_ep_dir)

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
