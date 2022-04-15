import json
import logging
import os
import shutil
from importlib.machinery import SourceFileLoader
from unittest.mock import ANY

import pytest
import requests
from globus_sdk import GlobusAPIError

from funcx_endpoint.endpoint.endpoint import Endpoint

logger = logging.getLogger("mock_funcx")


def _fake_http_response(*, status: int = 200, method: str = "GET") -> requests.Response:
    req = requests.Request(method, "https://funcx.example.org/")
    p_req = req.prepare()
    res = requests.Response()
    res.request = p_req
    res.status_code = status
    return res


class TestStart:
    @pytest.fixture(autouse=True)
    def test_setup_teardown(self):
        # Code that will run before your test, for example:

        funcx_dir = f"{os.getcwd()}"
        config_dir = os.path.join(funcx_dir, "mock_endpoint")
        assert not os.path.exists(config_dir)
        # A test function will be run at this point
        yield
        # Code that will run after your test, for example:
        shutil.rmtree(config_dir)

    def test_configure(self):
        manager = Endpoint(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")
        manager.configure_endpoint("mock_endpoint", None)
        assert os.path.exists(config_dir)

    def test_double_configure(self):
        manager = Endpoint(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        assert os.path.exists(config_dir)
        with pytest.raises(Exception, match="ConfigExists"):
            manager.configure_endpoint("mock_endpoint", None)

    @pytest.mark.skip(
        "This test needs to be re-written after endpoint_register is updated"
    )
    def test_start(self, mocker):
        mock_client = mocker.patch("funcx_endpoint.endpoint.endpoint.FuncXClient")
        reg_info = {
            "endpoint_id": "abcde12345",
            "address": "localhost",
            "client_ports": "8080",
        }
        mock_client.return_value.register_endpoint.return_value = reg_info

        mock_zmq_create = mocker.patch(
            "zmq.auth.create_certificates", return_value=("public/key/file", None)
        )
        mock_zmq_load = mocker.patch(
            "zmq.auth.load_certificate",
            return_value=(b"12345abcde", b"12345abcde"),
        )

        mock_context = mocker.patch("daemon.DaemonContext")

        # Allow this mock to be used in a with statement
        mock_context.return_value.__enter__.return_value = None
        mock_context.return_value.__exit__.return_value = None

        mock_context.return_value.pidfile.path = ""

        mock_daemon = mocker.patch.object(Endpoint, "daemon_launch", return_value=None)

        mock_uuid = mocker.patch("funcx_endpoint.endpoint.endpoint.uuid.uuid4")
        mock_uuid.return_value = 123456

        mock_pidfile = mocker.patch(
            "funcx_endpoint.endpoint.endpoint.daemon.pidfile.PIDLockFile"
        )
        mock_pidfile.return_value = None

        mock_results_ack_handler = mocker.patch(
            "funcx_endpoint.endpoint.endpoint.ResultsAckHandler"
        )

        manager = Endpoint(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        endpoint_config = SourceFileLoader(
            "config", os.path.join(config_dir, "config.py")
        ).load_module()
        manager.start_endpoint("mock_endpoint", None, endpoint_config)

        mock_zmq_create.assert_called_with(
            os.path.join(config_dir, "certificates"), "endpoint"
        )
        mock_zmq_load.assert_called_with("public/key/file")

        funcx_client_options = {
            "funcx_service_address": endpoint_config.config.funcx_service_address,
        }

        mock_daemon.assert_called_with(
            "123456",
            config_dir,
            os.path.join(config_dir, "certificates"),
            endpoint_config,
            reg_info,
            funcx_client_options,
            mock_results_ack_handler.return_value,
        )

        mock_context.assert_called_with(
            working_directory=config_dir,
            umask=0o002,
            pidfile=None,
            stdout=ANY,  # open(os.path.join(config_dir, './interchange.stdout'), 'w+'),
            stderr=ANY,  # open(os.path.join(config_dir, './interchange.stderr'), 'w+'),
            detach_process=True,
        )

    @pytest.mark.skip(
        "This test needs to be re-written after endpoint_register is updated"
    )
    def test_start_registration_error(self, mocker):
        """This tests what happens if a 400 error response comes back from the
        initial endpoint registration. It is expected that this exception should
        be raised during endpoint start. mock_zmq_create and mock_zmq_load are
        being asserted against because this zmq setup happens before registration
        occurs.
        """
        mocker.patch("funcx_endpoint.endpoint.endpoint.FuncXClient")

        mock_register_endpoint = mocker.patch(
            "funcx_endpoint.endpoint.endpoint.register_endpoint"
        )
        mock_register_endpoint.side_effect = GlobusAPIError(
            _fake_http_response(status=400, method="POST")
        )

        mock_zmq_create = mocker.patch(
            "zmq.auth.create_certificates", return_value=("public/key/file", None)
        )
        mock_zmq_load = mocker.patch(
            "zmq.auth.load_certificate",
            return_value=(b"12345abcde", b"12345abcde"),
        )

        mock_uuid = mocker.patch("funcx_endpoint.endpoint.endpoint.uuid.uuid4")
        mock_uuid.return_value = 123456

        mock_pidfile = mocker.patch(
            "funcx_endpoint.endpoint.endpoint.daemon.pidfile.PIDLockFile"
        )
        mock_pidfile.return_value = None

        mocker.patch("funcx_endpoint.endpoint.endpoint.ResultsAckHandler")

        manager = Endpoint(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        endpoint_config = SourceFileLoader(
            "config", os.path.join(config_dir, "config.py")
        ).load_module()

        with pytest.raises(GlobusAPIError):
            manager.start_endpoint("mock_endpoint", None, endpoint_config)

        mock_zmq_create.assert_called_with(
            os.path.join(config_dir, "certificates"), "endpoint"
        )
        mock_zmq_load.assert_called_with("public/key/file")

    @pytest.mark.skip(
        "This test needs to be re-written after endpoint_register is updated"
    )
    def test_start_registration_5xx_error(self, mocker):
        """
        This tests what happens if a 500 error response comes back from the initial
        endpoint registration.

        It is expected that this exception should NOT be raised and that the interchange
        should be started without any registration info being passed in. The
        registration should then be retried in the interchange daemon, because a 5xx
        error suggests that there is a temporary service issue that will resolve on its
        own. mock_zmq_create and mock_zmq_load are being asserted against because this
        zmq setup happens before registration occurs.
        """
        mocker.patch("funcx_endpoint.endpoint.endpoint.FuncXClient")

        mock_register_endpoint = mocker.patch(
            "funcx_endpoint.endpoint.endpoint.register_endpoint"
        )
        mock_register_endpoint.side_effect = GlobusAPIError(
            _fake_http_response(status=500, method="POST")
        )

        mock_zmq_create = mocker.patch(
            "zmq.auth.create_certificates", return_value=("public/key/file", None)
        )
        mock_zmq_load = mocker.patch(
            "zmq.auth.load_certificate",
            return_value=(b"12345abcde", b"12345abcde"),
        )

        mock_context = mocker.patch("daemon.DaemonContext")

        # Allow this mock to be used in a with statement
        mock_context.return_value.__enter__.return_value = None
        mock_context.return_value.__exit__.return_value = None

        mock_context.return_value.pidfile.path = ""

        mock_daemon = mocker.patch.object(Endpoint, "daemon_launch", return_value=None)

        mock_uuid = mocker.patch("funcx_endpoint.endpoint.endpoint.uuid.uuid4")
        mock_uuid.return_value = 123456

        mock_pidfile = mocker.patch(
            "funcx_endpoint.endpoint.endpoint.daemon.pidfile.PIDLockFile"
        )
        mock_pidfile.return_value = None

        mock_results_ack_handler = mocker.patch(
            "funcx_endpoint.endpoint.endpoint.ResultsAckHandler"
        )

        manager = Endpoint(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        endpoint_config = SourceFileLoader(
            "config", os.path.join(config_dir, "config.py")
        ).load_module()

        manager.start_endpoint("mock_endpoint", None, endpoint_config)

        mock_zmq_create.assert_called_with(
            os.path.join(config_dir, "certificates"), "endpoint"
        )
        mock_zmq_load.assert_called_with("public/key/file")

        funcx_client_options = {
            "funcx_service_address": endpoint_config.config.funcx_service_address,
        }

        # We should expect reg_info in this test to be None when passed into
        # daemon_launch because a 5xx GlobusAPIError was raised during registration
        reg_info = None
        mock_daemon.assert_called_with(
            "123456",
            config_dir,
            os.path.join(config_dir, "certificates"),
            endpoint_config,
            reg_info,
            funcx_client_options,
            mock_results_ack_handler.return_value,
        )

        mock_context.assert_called_with(
            working_directory=config_dir,
            umask=0o002,
            pidfile=None,
            stdout=ANY,  # open(os.path.join(config_dir, './interchange.stdout'), 'w+'),
            stderr=ANY,  # open(os.path.join(config_dir, './interchange.stderr'), 'w+'),
            detach_process=True,
        )

    def test_start_without_executors(self, mocker):
        mock_client = mocker.patch("funcx_endpoint.endpoint.endpoint.FuncXClient")
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

        class mock_load:
            class mock_executors:
                executors = None

            config = mock_executors()

        manager = Endpoint(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        with pytest.raises(
            Exception,
            match=f"Endpoint config file at {config_dir} is "
            "missing executor definitions",
        ):
            manager.start_endpoint("mock_endpoint", None, mock_load())

    @pytest.mark.skip("This test doesn't make much sense")
    def test_daemon_launch(self, mocker):
        mock_interchange = mocker.patch(
            "funcx_endpoint.endpoint.endpoint.EndpointInterchange"
        )
        mock_interchange.return_value.start.return_value = None
        mock_interchange.return_value.stop.return_value = None

        manager = Endpoint(funcx_dir=os.getcwd())
        manager.name = "test"
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        mock_optionals = {}
        mock_optionals["logdir"] = config_dir

        manager.configure_endpoint("mock_endpoint", None)
        endpoint_config = SourceFileLoader(
            "config", os.path.join(config_dir, "config.py")
        ).load_module()

        funcx_client_options = None

        manager.daemon_launch(
            endpoint_uuid="mock_endpoint_uuid",
            endpoint_dir=config_dir,
            endpoint_config=endpoint_config,
            reg_info=None,
            funcx_client_options=funcx_client_options,
            results_ack_handler=None,
        )

        mock_interchange.assert_called_with(
            endpoint_config.config,
            reg_info=None,
            endpoint_uuid="mock_endpoint_uuid",
            endpoint_dir=config_dir,
            funcx_client_options=funcx_client_options,
            results_ack_handler=None,
            **mock_optionals,
        )

    @pytest.mark.skip(
        "This test needs to be re-written after endpoint_register is updated"
    )
    def test_with_funcx_config(self, mocker):
        mock_interchange = mocker.patch(
            "funcx_endpoint.endpoint.interchange.EndpointInterchange"
        )
        mock_interchange.return_value.start.return_value = None
        mock_interchange.return_value.stop.return_value = None

        mock_optionals = {}
        mock_optionals["interchange_address"] = "127.0.0.1"

        mock_funcx_config = {}
        mock_funcx_config["endpoint_address"] = "127.0.0.1"

        manager = Endpoint(funcx_dir=os.getcwd())
        manager.name = "test"
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")
        mock_optionals["logdir"] = config_dir
        manager.funcx_config = mock_funcx_config

        manager.configure_endpoint("mock_endpoint", None)
        endpoint_config = SourceFileLoader(
            "config", os.path.join(config_dir, "config.py")
        ).load_module()

        funcx_client_options = {}

        manager.daemon_launch(
            "mock_endpoint_uuid",
            config_dir,
            "mock_keys_dir",
            endpoint_config,
            None,
            funcx_client_options,
            None,
        )

        mock_interchange.assert_called_with(
            endpoint_config.config,
            endpoint_id="mock_endpoint_uuid",
            keys_dir="mock_keys_dir",
            endpoint_dir=config_dir,
            endpoint_name=manager.name,
            reg_info=None,
            funcx_client_options=funcx_client_options,
            results_ack_handler=None,
            **mock_optionals,
        )

    def test_check_endpoint_json_no_json_no_uuid(self, mocker):
        mock_uuid = mocker.patch("funcx_endpoint.endpoint.endpoint.uuid.uuid4")
        mock_uuid.return_value = 123456

        manager = Endpoint(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        assert "123456" == manager.check_endpoint_json(
            os.path.join(config_dir, "endpoint.json"), None
        )

    def test_check_endpoint_json_no_json_given_uuid(self, mocker):
        manager = Endpoint(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        assert "234567" == manager.check_endpoint_json(
            os.path.join(config_dir, "endpoint.json"), "234567"
        )

    def test_check_endpoint_json_given_json(self, mocker):
        manager = Endpoint(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)

        mock_dict = {"endpoint_id": "abcde12345"}
        with open(os.path.join(config_dir, "endpoint.json"), "w") as fd:
            json.dump(mock_dict, fd)

        assert "abcde12345" == manager.check_endpoint_json(
            os.path.join(config_dir, "endpoint.json"), "234567"
        )
