import logging
import os
import pathlib
import shutil
from unittest.mock import patch

import globus_compute_endpoint.endpoint
import pytest
import requests
import yaml
from globus_compute_endpoint.endpoint.endpoint import Endpoint, UserEndpointConfig
from globus_sdk import GlobusAPIError

logger = logging.getLogger("mock_funcx")

_MOCK_BASE = "globus_compute_endpoint.endpoint.endpoint."
DEF_CONFIG_DIR = (
    pathlib.Path(globus_compute_endpoint.endpoint.config.__file__).resolve().parent
)


def _fake_http_response(*, status: int = 200, method: str = "GET") -> requests.Response:
    req = requests.Request(method, "https://funcx.example.org/")
    p_req = req.prepare()
    res = requests.Response()
    res.request = p_req
    res.status_code = status
    return res


class TestStart:
    @pytest.fixture(autouse=True)
    def test_setup_teardown(self, fs):
        funcx_dir = f"{os.getcwd()}"
        config_dir = pathlib.Path(funcx_dir) / "mock_endpoint"
        assert not config_dir.exists()
        # pyfakefs will take care of newly created files, not existing config
        fs.add_real_file(DEF_CONFIG_DIR / "default_config.yaml")
        fs.add_real_file(DEF_CONFIG_DIR / "user_config_template.yaml.j2")
        fs.add_real_file(DEF_CONFIG_DIR / "user_config_schema.json")
        fs.add_real_file(DEF_CONFIG_DIR / "user_environment.yaml")
        fs.add_real_file(DEF_CONFIG_DIR / "example_identity_mapping_config.json")

        yield

    def test_configure(self):
        config_dir = pathlib.Path("/some/path/mock_endpoint")
        Endpoint.configure_endpoint(config_dir, None)
        assert config_dir.exists() and config_dir.is_dir()

    def test_double_configure(self):
        config_dir = pathlib.Path("/some/path/mock_endpoint")

        Endpoint.configure_endpoint(config_dir, None)
        assert config_dir.exists() and config_dir.is_dir()
        with pytest.raises(Exception, match="ConfigExists"):
            Endpoint.configure_endpoint(config_dir, None)

    @pytest.mark.parametrize("ha", (None, True, False))
    def test_configure_ha_existing_config(self, ha):
        config_dir = pathlib.Path("/some/path/mock_endpoint")
        config_file = Endpoint._config_file_path(config_dir)
        config_copy = str(config_dir.parent / "config2.yaml")

        # First, make an entry with no ha
        Endpoint.configure_endpoint(config_dir, None, high_assurance=False)
        shutil.move(config_file, config_copy)
        shutil.rmtree(config_dir)

        # Then, modify it with new settings
        Endpoint.configure_endpoint(config_dir, config_copy, high_assurance=ha)

        with open(config_file) as f:
            config_dict = yaml.safe_load(f)
        assert ("high_assurance" in config_dict) is (ha is True)

        os.remove(config_copy)

    @pytest.mark.parametrize("ha", (False, True))
    def test_configure_ha_audit_default(self, ha):
        config_dir = pathlib.Path("/some/path/mock_endpoint")
        config_file = Endpoint._config_file_path(config_dir)
        audit_path = Endpoint._audit_log_path(config_dir)

        with patch(f"{_MOCK_BASE}print"):  # quiet, please
            Endpoint.configure_endpoint(config_dir, None, high_assurance=ha)

        assert config_file.exists(), "Verify setup"
        conf = yaml.safe_load(config_file.read_text())
        if not ha:
            assert "audit_log_path" not in conf
        else:
            assert conf.get("high_assurance"), conf
            assert conf.get("audit_log_path") == str(audit_path), conf

    def test_start_without_engine(self, mocker):
        mock_client = mocker.patch(f"{_MOCK_BASE}Client")
        mock_client.return_value.register_endpoint.return_value = {
            "endpoint_id": "abcde12345",
            "address": "localhost",
            "client_ports": "8080",
        }

        mock_context = mocker.patch("daemon.DaemonContext")

        # Allow this mock to be used in a with statement
        mock_context.return_value.__enter__.return_value = None
        mock_context.return_value.__exit__.return_value = None
        mock_context.return_value.pidfile.path = ""

        config = UserEndpointConfig(detach_endpoint=False)

        config_dir = pathlib.Path("/some/path/mock_endpoint")

        Endpoint.configure_endpoint(config_dir, None)
        with pytest.raises(ValueError, match="has no engines defined"):
            log_to_console = False
            no_color = True
            Endpoint().start_endpoint(
                config_dir,
                None,
                config,
                log_to_console,
                no_color,
                reg_info={},
                ep_info={},
            )

    def test_start_ha_non_compliant(self, mocker):
        mock_client = mocker.patch(f"{_MOCK_BASE}Client")
        mock_client.return_value.register_endpoint.return_value = {
            "endpoint_id": "abcde12345",
            "address": "localhost",
            "client_ports": "8080",
        }

        mock_context = mocker.patch("daemon.DaemonContext")

        # Allow this mock to be used in a with statement
        mock_context.return_value.__enter__.return_value = None
        mock_context.return_value.__exit__.return_value = None
        mock_context.return_value.pidfile.path = ""

        mock_engine = mocker.Mock()
        mock_engine.assert_ha_compliant = mocker.Mock(side_effect=ValueError("foo"))

        config = UserEndpointConfig(
            detach_endpoint=False, high_assurance=True, engine=mock_engine
        )

        config_dir = pathlib.Path("/some/path/mock_endpoint")

        Endpoint.configure_endpoint(config_dir, None)
        with pytest.raises(
            ValueError, match="Engine configuration is not High Assurance compliant"
        ):
            log_to_console = False
            no_color = True
            Endpoint().start_endpoint(
                config_dir,
                None,
                config,
                log_to_console,
                no_color,
                reg_info={},
                ep_info={},
            )

    @pytest.mark.parametrize("web_svc_ok", (True, False))
    @pytest.mark.parametrize("force", (True, False))
    def test_delete_endpoint(self, mocker, web_svc_ok, force, ep_uuid):
        config_dir = pathlib.Path("/some/path/mock_endpoint")

        mock_stop_endpoint = mocker.patch.object(Endpoint, "stop_endpoint")
        mock_rmtree = mocker.patch.object(shutil, "rmtree")

        Endpoint.configure_endpoint(config_dir, None)

        mock_gcc = mocker.Mock()
        mocker.patch(f"{_MOCK_BASE}Endpoint.get_funcx_client").return_value = mock_gcc

        # Exit if the web service call fails and we're not force deleting
        if not web_svc_ok:
            exc = GlobusAPIError(_fake_http_response(status=500, method="POST"))
            mock_gcc.delete_endpoint.side_effect = exc
            mock_gcc.get_endopint_status.side_effect = exc

            if not force:
                with pytest.raises(SystemExit), patch(f"{_MOCK_BASE}log") as mock_log:
                    Endpoint.delete_endpoint(config_dir, force=force, ep_uuid=ep_uuid)
                a, _k = mock_log.critical.call_args
                assert "without deleting the local endpoint" in a[0], "expected notice"

                assert not mock_stop_endpoint.called
                assert not mock_rmtree.called
                return
        else:
            mock_gcc.get_endpoint_status.return_value = {
                # "offline" is tested in test_endpoint_unit
                "status": "online"
            }

        try:
            Endpoint.delete_endpoint(
                config_dir, ep_config=None, force=force, ep_uuid=ep_uuid
            )

            if web_svc_ok:
                mock_stop_endpoint.assert_called_with(config_dir, None, remote=False)
            assert mock_gcc.delete_endpoint.called
            assert mock_gcc.delete_endpoint.call_args[0][0] == ep_uuid
            mock_rmtree.assert_called_with(config_dir)
        except SystemExit as e:
            # If currently running, error out if force is not specified
            # the message str itself is confirmed in test_endpoint_unit
            assert not force
            assert e.code == -1
