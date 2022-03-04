import logging
import os
import shutil
from importlib.machinery import SourceFileLoader

import pytest

from funcx_endpoint.endpoint.endpoint import Endpoint
from funcx_endpoint.endpoint.interchange import EndpointInterchange

logger = logging.getLogger("mock_funcx")


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

    def test_endpoint_id(self, mocker):
        mock_client = mocker.patch("funcx_endpoint.endpoint.interchange.FuncXClient")
        mock_client.return_value = None

        manager = Endpoint(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")
        keys_dir = os.path.join(config_dir, "certificates")

        optionals = {}
        optionals["client_address"] = "127.0.0.1"
        optionals["client_ports"] = (8080, 8081, 8082)
        optionals["logdir"] = "./mock_endpoint"

        manager.configure_endpoint("mock_endpoint", None)
        endpoint_config = SourceFileLoader(
            "config", os.path.join(config_dir, "config.py")
        ).load_module()

        for executor in endpoint_config.config.executors:
            executor.passthrough = False

        ic = EndpointInterchange(
            endpoint_config.config,
            endpoint_id="mock_endpoint_id",
            keys_dir=keys_dir,
            reg_info="amqp://guest:guest@localhost:5672/%2F",
            **optionals,
        )

        for executor in ic.executors.values():
            assert executor.endpoint_id == "mock_endpoint_id"

    def test_register_endpoint(self, mocker):
        mock_client = mocker.patch("funcx_endpoint.endpoint.interchange.FuncXClient")
        mock_client.return_value = None

        mock_register_endpoint = mocker.patch(
            "funcx_endpoint.endpoint.interchange.register_endpoint"
        )
        mock_register_endpoint.return_value = {
            "endpoint_id": "abcde12345",
            "public_ip": "127.0.0.1",
            "tasks_port": 8080,
            "results_port": 8081,
            "commands_port": 8082,
        }

        manager = Endpoint(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")
        keys_dir = os.path.join(config_dir, "certificates")

        optionals = {}
        optionals["client_address"] = "127.0.0.1"
        optionals["client_ports"] = (8080, 8081, 8082)
        optionals["logdir"] = "./mock_endpoint"

        manager.configure_endpoint("mock_endpoint", None)
        endpoint_config = SourceFileLoader(
            "config", os.path.join(config_dir, "config.py")
        ).load_module()

        for executor in endpoint_config.config.executors:
            executor.passthrough = False

        ic = EndpointInterchange(
            endpoint_config.config,
            endpoint_id="mock_endpoint_id",
            keys_dir=keys_dir,
            reg_info="amqp://guest:guest@localhost:5672/%2F",
            **optionals,
        )

        ic.register_endpoint()
        assert ic.client_address == "127.0.0.1"
        assert ic.client_ports == (8080, 8081, 8082)

    def test_start_no_reg_info(self, mocker):
        mocker.patch("funcx_endpoint.endpoint.interchange.threading.Thread")

        mock_retry_call = mocker.patch("funcx_endpoint.endpoint.interchange.retry_call")

        mock_client = mocker.patch("funcx_endpoint.endpoint.interchange.FuncXClient")
        mock_client.return_value = None

        mock_register_endpoint = mocker.patch(
            "funcx_endpoint.endpoint.interchange.register_endpoint"
        )
        mock_register_endpoint.return_value = {
            "endpoint_id": "abcde12345",
            "public_ip": "127.0.0.1",
            "tasks_port": 8080,
            "results_port": 8081,
            "commands_port": 8082,
        }

        manager = Endpoint(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")
        keys_dir = os.path.join(config_dir, "certificates")

        optionals = {}
        optionals["client_address"] = "127.0.0.1"
        optionals["client_ports"] = (8080, 8081, 8082)
        optionals["logdir"] = "./mock_endpoint"

        manager.configure_endpoint("mock_endpoint", None)
        endpoint_config = SourceFileLoader(
            "config", os.path.join(config_dir, "config.py")
        ).load_module()

        for executor in endpoint_config.config.executors:
            executor.passthrough = False

        mock_quiesce = mocker.patch.object(
            EndpointInterchange, "quiesce", return_value=None
        )
        mock_main_loop = mocker.patch.object(
            EndpointInterchange, "_main_loop", return_value=None
        )

        ic = EndpointInterchange(
            endpoint_config.config,
            endpoint_id="mock_endpoint_id",
            keys_dir=keys_dir,
            reg_info="amqp://guest:guest@localhost:5672/%2F",
            **optionals,
        )

        ic.results_outgoing = mocker.Mock()

        # this must be set to force the retry loop in the start method to only run once
        ic._test_start = True
        ic.start()

        # we need to ensure that retry_call is called during interchange
        # start if reg_info has not been passed into the interchange
        mock_retry_call.assert_called()
        mock_quiesce.assert_called()
        mock_main_loop.assert_called()
