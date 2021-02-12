from funcx_endpoint.endpoint.endpoint_manager import EndpointManager
import os
import logging
import sys
import shutil
import pytest
import json
from pytest import fixture

logger = logging.getLogger('mock_funcx')


class TestStart:

    @pytest.fixture(autouse=True)
    def test_setup_teardown(self):
        # Code that will run before your test, for example:

        funcx_dir = f'{os.getcwd()}'
        config_dir = os.path.join(funcx_dir, "mock_endpoint")
        assert not os.path.exists(config_dir)
        # A test function will be run at this point
        yield
        # Code that will run after your test, for example:
        shutil.rmtree(config_dir)

    def test_configure(self):
        manager = EndpointManager(logger)
        manager.funcx_dir = f'{os.getcwd()}'
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        assert os.path.exists(config_dir)

    def test_double_configure(self):
        manager = EndpointManager(logger)
        manager.funcx_dir = f'{os.getcwd()}'
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        assert os.path.exists(config_dir)
        with pytest.raises(Exception, match='ConfigExists'):
            manager.configure_endpoint("mock_endpoint", None)

    def test_start(self, mocker):
        mock_client = mocker.patch("funcx_endpoint.endpoint.endpoint_manager.FuncXClient")
        mock_client.return_value.register_endpoint.return_value = {'endpoint_id': 'abcde12345',
                                                                   'address': 'localhost',
                                                                   'client_ports': '8080'}

        mock_zmq_create = mocker.patch("zmq.auth.create_certificates",
                                       return_value=("public/key/file", None))
        mock_zmq_load = mocker.patch("zmq.auth.load_certificate",
                                     return_value=("12345abcde".encode(), "12345abcde".encode()))

        mock_context = mocker.patch("daemon.DaemonContext")

        # Allow this mock to be used in a with statement
        mock_context.return_value.__enter__.return_value = None
        mock_context.return_value.__exit__.return_value = None

        mock_context.return_value.pidfile.path = ''

        mock_daemon = mocker.patch.object(EndpointManager, 'daemon_launch',
                                          return_value=None)

        manager = EndpointManager(logger)
        manager.funcx_dir = f'{os.getcwd()}'
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        manager.start_endpoint("mock_endpoint", None)

        mock_zmq_create.assert_called_with(os.path.join(config_dir, "certificates"), "endpoint")
        mock_zmq_load.assert_called_with("public/key/file")

        assert mock_daemon.call_count == 1
        args, kwargs = mock_daemon.call_args
        assert mock_client() in args
        assert config_dir in args
        assert os.path.join(config_dir, "certificates") in args

    def test_start_without_executors(self, mocker):
        mock_client = mocker.patch("funcx_endpoint.endpoint.endpoint_manager.FuncXClient")
        mock_client.return_value.register_endpoint.return_value = {'endpoint_id': 'abcde12345',
                                                                   'address': 'localhost',
                                                                   'client_ports': '8080'}

        mock_context = mocker.patch("daemon.DaemonContext")

        # Allow this mock to be used in a with statement
        mock_context.return_value.__enter__.return_value = None
        mock_context.return_value.__exit__.return_value = None

        mock_context.return_value.pidfile.path = ''

        class mock_load():
            def load_module(self):
                class mock_config():
                    def __init__(self):
                        class mock_executors():
                            def __init__(self):
                                self.executors = None
                        self.config = mock_executors()
                return mock_config()

        mock_config_load = mocker.patch('funcx_endpoint.endpoint.endpoint_manager.SourceFileLoader')
        mock_config_load.return_value = mock_load()

        manager = EndpointManager(logger)
        manager.funcx_dir = f'{os.getcwd()}'
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        with pytest.raises(Exception, match=f'Endpoint config file at {config_dir} is missing executor definitions'):
            manager.start_endpoint("mock_endpoint", None)

    def test_daemon_launch(self, mocker):
        mock_register_endpoint = mocker.patch.object(EndpointManager, 'register_endpoint',
                                                     return_value={'endpoint_id': 'abcde12345',
                                                                   'public_ip': '127.0.0.1',
                                                                   'tasks_port': 8080,
                                                                   'results_port': 8081,
                                                                   'commands_port': 8082, })

        mock_interchange = mocker.patch('funcx_endpoint.endpoint.endpoint_manager.EndpointInterchange')
        mock_interchange.return_value.start.return_value = None
        mock_interchange.return_vlaue.end.return_value = None

        mock_client = mocker.patch("funcx_endpoint.endpoint.endpoint_manager.FuncXClient")

        manager = EndpointManager(logger)
        manager.funcx_dir = f'{os.getcwd()}'
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        manager.daemon_launch(mock_client(), 'mock_endpoint_uuid', config_dir, 'mock_keys_dir')

    def test_register_endpoint(self, mocker):
        mock_client = mocker.patch("funcx_endpoint.endpoint.endpoint_manager.FuncXClient")
        mock_client.return_value.register_endpoint.return_value = {'status': 'okay',
                                                                   'endpoint_id': 'mock_endpoint_id',
                                                                   'forwarder_pubkey': 'abcde12345'}

        manager = EndpointManager(logger)
        manager.funcx_dir = f'{os.getcwd()}'
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        manager.register_endpoint(mock_client(), 'mock_endpoint_uuid', config_dir)

    def test_register_endpoint_status_error(self, mocker):
        mock_client = mocker.patch("funcx_endpoint.endpoint.endpoint_manager.FuncXClient")
        mock_client.return_value.register_endpoint.return_value = {'status': 'error',
                                                                   'reason': 'unknown'}

        manager = EndpointManager(logger)
        manager.funcx_dir = f'{os.getcwd()}'
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        with pytest.raises(Exception, match='Endpoint registration failed. Service fail reason provided: unknown'):
            manager.register_endpoint(mock_client(), 'mock_endpoint_uuid', config_dir)

    def test_register_endpoint_no_endpoint_id(self, mocker):
        mock_client = mocker.patch("funcx_endpoint.endpoint.endpoint_manager.FuncXClient")
        mock_client.return_value.register_endpoint.return_value = {'status': 'okay'}

        manager = EndpointManager(logger)
        manager.funcx_dir = f'{os.getcwd()}'
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        with pytest.raises(Exception, match='Endpoint ID was not included in the service\'s registration response.'):
            manager.register_endpoint(mock_client(), 'mock_endpoint_uuid', config_dir)

    def test_register_endpoint_int_endpoint_id(self, mocker):
        mock_client = mocker.patch("funcx_endpoint.endpoint.endpoint_manager.FuncXClient")
        mock_client.return_value.register_endpoint.return_value = {'status': 'okay',
                                                                   'endpoint_id': 123456}

        manager = EndpointManager(logger)
        manager.funcx_dir = f'{os.getcwd()}'
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        with pytest.raises(Exception, match='Endpoint ID sent by the service was not a string.'):
            manager.register_endpoint(mock_client(), 'mock_endpoint_uuid', config_dir)

    def test_check_endpoint_json_no_json_no_uuid(self, mocker):
        mock_uuid = mocker.patch('funcx_endpoint.endpoint.endpoint_manager.uuid.uuid4', return_value=123456)

        manager = EndpointManager(logger)
        manager.funcx_dir = f'{os.getcwd()}'
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        assert '123456' == manager.check_endpoint_json(os.path.join(config_dir, 'endpoint.json'), None)

    def test_check_endpoint_json_no_json_given_uuid(self, mocker):
        manager = EndpointManager(logger)
        manager.funcx_dir = f'{os.getcwd()}'
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        assert '234567' == manager.check_endpoint_json(os.path.join(config_dir, 'endpoint.json'), '234567')

    def test_check_endpoint_json_given_json(self, mocker):
        manager = EndpointManager(logger)
        manager.funcx_dir = f'{os.getcwd()}'
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)

        mock_dict = {'endpoint_id': 'abcde12345'}
        with open(os.path.join(config_dir, 'endpoint.json'), "w") as fd:
            json.dump(mock_dict, fd)

        assert 'abcde12345' == manager.check_endpoint_json(os.path.join(config_dir, 'endpoint.json'), '234567')
