from funcx_endpoint.endpoint.endpoint_manager import EndpointManager
from importlib.machinery import SourceFileLoader
import os
import logging
import sys
import shutil
import pytest
import json
from pytest import fixture
from unittest.mock import ANY
from globus_sdk import GlobusHTTPResponse, GlobusAPIError
from requests import Response

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
        manager = EndpointManager(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")
        manager.configure_endpoint("mock_endpoint", None)
        assert os.path.exists(config_dir)

    def test_double_configure(self):
        manager = EndpointManager(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        assert os.path.exists(config_dir)
        with pytest.raises(Exception, match='ConfigExists'):
            manager.configure_endpoint("mock_endpoint", None)

    def test_start(self, mocker):
        mock_client = mocker.patch("funcx_endpoint.endpoint.endpoint_manager.FuncXClient")
        reg_info = {'endpoint_id': 'abcde12345',
                    'address': 'localhost',
                    'client_ports': '8080'}
        mock_client.return_value.register_endpoint.return_value = reg_info

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

        mock_uuid = mocker.patch('funcx_endpoint.endpoint.endpoint_manager.uuid.uuid4')
        mock_uuid.return_value = 123456

        mock_pidfile = mocker.patch('funcx_endpoint.endpoint.endpoint_manager.daemon.pidfile.PIDLockFile')
        mock_pidfile.return_value = None

        manager = EndpointManager(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        endpoint_config = SourceFileLoader('config',
                                           os.path.join(config_dir, 'config.py')).load_module()
        manager.start_endpoint("mock_endpoint", None, endpoint_config)

        mock_zmq_create.assert_called_with(os.path.join(config_dir, "certificates"), "endpoint")
        mock_zmq_load.assert_called_with("public/key/file")

        funcx_client_options = {
            "funcx_service_address": endpoint_config.config.funcx_service_address,
            "check_endpoint_version": True,
        }

        mock_daemon.assert_called_with('123456', config_dir, os.path.join(config_dir, "certificates"), endpoint_config, reg_info, funcx_client_options)

        mock_context.assert_called_with(working_directory=config_dir,
                                        umask=0o002,
                                        pidfile=None,
                                        stdout=ANY,  # open(os.path.join(config_dir, './interchange.stdout'), 'w+'),
                                        stderr=ANY,  # open(os.path.join(config_dir, './interchange.stderr'), 'w+'),
                                        detach_process=True)

    def test_start_registration_error(self, mocker):
        mocker.patch("funcx_endpoint.endpoint.endpoint_manager.FuncXClient")

        base_r = Response()
        base_r.headers = {
            "Content-Type": "json"
        }
        base_r.status_code = 400
        r = GlobusHTTPResponse(base_r)
        r.status_code = base_r.status_code
        r.headers = base_r.headers

        mock_register_endpoint = mocker.patch('funcx_endpoint.endpoint.endpoint_manager.register_endpoint')
        mock_register_endpoint.side_effect = GlobusAPIError(r)

        mock_zmq_create = mocker.patch("zmq.auth.create_certificates",
                                       return_value=("public/key/file", None))
        mock_zmq_load = mocker.patch("zmq.auth.load_certificate",
                                     return_value=("12345abcde".encode(), "12345abcde".encode()))

        mock_uuid = mocker.patch('funcx_endpoint.endpoint.endpoint_manager.uuid.uuid4')
        mock_uuid.return_value = 123456

        mock_pidfile = mocker.patch('funcx_endpoint.endpoint.endpoint_manager.daemon.pidfile.PIDLockFile')
        mock_pidfile.return_value = None

        manager = EndpointManager(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        endpoint_config = SourceFileLoader('config',
                                           os.path.join(config_dir, 'config.py')).load_module()

        with pytest.raises(GlobusAPIError):
            manager.start_endpoint("mock_endpoint", None, endpoint_config)

        mock_zmq_create.assert_called_with(os.path.join(config_dir, "certificates"), "endpoint")
        mock_zmq_load.assert_called_with("public/key/file")

    def test_start_registration_5xx_error(self, mocker):
        mocker.patch("funcx_endpoint.endpoint.endpoint_manager.FuncXClient")

        base_r = Response()
        base_r.headers = {
            "Content-Type": "json"
        }
        base_r.status_code = 500
        r = GlobusHTTPResponse(base_r)
        r.status_code = base_r.status_code
        r.headers = base_r.headers

        mock_register_endpoint = mocker.patch('funcx_endpoint.endpoint.endpoint_manager.register_endpoint')
        mock_register_endpoint.side_effect = GlobusAPIError(r)

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

        mock_uuid = mocker.patch('funcx_endpoint.endpoint.endpoint_manager.uuid.uuid4')
        mock_uuid.return_value = 123456

        mock_pidfile = mocker.patch('funcx_endpoint.endpoint.endpoint_manager.daemon.pidfile.PIDLockFile')
        mock_pidfile.return_value = None

        manager = EndpointManager(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        endpoint_config = SourceFileLoader('config',
                                           os.path.join(config_dir, 'config.py')).load_module()

        manager.start_endpoint("mock_endpoint", None, endpoint_config)

        mock_zmq_create.assert_called_with(os.path.join(config_dir, "certificates"), "endpoint")
        mock_zmq_load.assert_called_with("public/key/file")

        funcx_client_options = {
            "funcx_service_address": endpoint_config.config.funcx_service_address,
            "check_endpoint_version": True,
        }

        # We should expect reg_info in this test to be None when passed into daemon_launch
        # because a 5xx GlobusAPIError was raised during registration
        reg_info = None
        mock_daemon.assert_called_with('123456', config_dir, os.path.join(config_dir, "certificates"), endpoint_config, reg_info, funcx_client_options)

        mock_context.assert_called_with(working_directory=config_dir,
                                        umask=0o002,
                                        pidfile=None,
                                        stdout=ANY,  # open(os.path.join(config_dir, './interchange.stdout'), 'w+'),
                                        stderr=ANY,  # open(os.path.join(config_dir, './interchange.stderr'), 'w+'),
                                        detach_process=True)

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
            class mock_executors():
                executors = None
            config = mock_executors()

        manager = EndpointManager(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        with pytest.raises(Exception, match=f'Endpoint config file at {config_dir} is missing executor definitions'):
            manager.start_endpoint("mock_endpoint", None, mock_load())

    def test_daemon_launch(self, mocker):
        mock_interchange = mocker.patch('funcx_endpoint.endpoint.endpoint_manager.EndpointInterchange')
        mock_interchange.return_value.start.return_value = None
        mock_interchange.return_value.stop.return_value = None

        manager = EndpointManager(funcx_dir=os.getcwd())
        manager.name = 'test'
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        mock_optionals = {}
        mock_optionals['logdir'] = config_dir

        manager.configure_endpoint("mock_endpoint", None)
        endpoint_config = SourceFileLoader('config',
                                           os.path.join(config_dir, 'config.py')).load_module()

        funcx_client_options = {}

        manager.daemon_launch('mock_endpoint_uuid', config_dir, 'mock_keys_dir', endpoint_config, None, funcx_client_options)

        mock_interchange.assert_called_with(endpoint_config.config,
                                            endpoint_id='mock_endpoint_uuid',
                                            keys_dir='mock_keys_dir',
                                            endpoint_dir=config_dir,
                                            endpoint_name=manager.name,
                                            reg_info=None,
                                            funcx_client_options=funcx_client_options,
                                            **mock_optionals)

    def test_with_funcx_config(self, mocker):
        mock_interchange = mocker.patch('funcx_endpoint.endpoint.endpoint_manager.EndpointInterchange')
        mock_interchange.return_value.start.return_value = None
        mock_interchange.return_value.stop.return_value = None

        mock_optionals = {}
        mock_optionals['interchange_address'] = '127.0.0.1'

        mock_funcx_config = {}
        mock_funcx_config['endpoint_address'] = '127.0.0.1'

        manager = EndpointManager(funcx_dir=os.getcwd())
        manager.name = 'test'
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")
        mock_optionals['logdir'] = config_dir
        manager.funcx_config = mock_funcx_config

        manager.configure_endpoint("mock_endpoint", None)
        endpoint_config = SourceFileLoader('config',
                                           os.path.join(config_dir, 'config.py')).load_module()

        funcx_client_options = {}

        manager.daemon_launch('mock_endpoint_uuid', config_dir, 'mock_keys_dir', endpoint_config, None, funcx_client_options)

        mock_interchange.assert_called_with(endpoint_config.config,
                                            endpoint_id='mock_endpoint_uuid',
                                            keys_dir='mock_keys_dir',
                                            endpoint_dir=config_dir,
                                            endpoint_name=manager.name,
                                            reg_info=None,
                                            funcx_client_options=funcx_client_options,
                                            **mock_optionals)

    def test_check_endpoint_json_no_json_no_uuid(self, mocker):
        mock_uuid = mocker.patch('funcx_endpoint.endpoint.endpoint_manager.uuid.uuid4')
        mock_uuid.return_value = 123456

        manager = EndpointManager(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        assert '123456' == manager.check_endpoint_json(os.path.join(config_dir, 'endpoint.json'), None)

    def test_check_endpoint_json_no_json_given_uuid(self, mocker):
        manager = EndpointManager(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)
        assert '234567' == manager.check_endpoint_json(os.path.join(config_dir, 'endpoint.json'), '234567')

    def test_check_endpoint_json_given_json(self, mocker):
        manager = EndpointManager(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")

        manager.configure_endpoint("mock_endpoint", None)

        mock_dict = {'endpoint_id': 'abcde12345'}
        with open(os.path.join(config_dir, 'endpoint.json'), "w") as fd:
            json.dump(mock_dict, fd)

        assert 'abcde12345' == manager.check_endpoint_json(os.path.join(config_dir, 'endpoint.json'), '234567')
