from funcx_endpoint.endpoint.endpoint_manager import EndpointManager
# import zmq
import os
import logging
import sys
import shutil
import pytest
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
