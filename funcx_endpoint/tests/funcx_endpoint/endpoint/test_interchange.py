from funcx_endpoint.endpoint.endpoint_manager import EndpointManager
from funcx_endpoint.endpoint.interchange import EndpointInterchange
from importlib.machinery import SourceFileLoader
import os
import logging
import sys
import shutil
import pytest
import json
from pytest import fixture
from unittest.mock import ANY

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

    def test_endpoint_id(self, mocker):
        mock_taskqueue = mocker.patch('funcx_endpoint.endpoint.interchange.TaskQueue')
        mock_taskqueue.return_value.put.return_value = None

        mock_client = mocker.patch("funcx_endpoint.endpoint.interchange.FuncXClient")
        mock_client.return_value = None

        mock_queue = mocker.patch("funcx_endpoint.endpoint.interchange.mpQueue")
        mock_queue.return_value = None

        manager = EndpointManager(funcx_dir=os.getcwd())
        config_dir = os.path.join(manager.funcx_dir, "mock_endpoint")
        keys_dir = os.path.join(config_dir, 'certificates')

        optionals = {}
        optionals['client_address'] = '127.0.0.1'
        optionals['client_ports'] = (8080, 8081, 8082)
        optionals['logdir'] = './mock_endpoint'

        manager.configure_endpoint("mock_endpoint", None)
        endpoint_config = SourceFileLoader('config',
                                           os.path.join(config_dir, 'config.py')).load_module()

        for executor in endpoint_config.config.executors:
            executor.passthrough = False

        ic = EndpointInterchange(endpoint_config.config,
                                 endpoint_id='mock_endpoint_id',
                                 keys_dir=keys_dir,
                                 **optionals)

        for executor in ic.executors.values():
            assert executor.endpoint_id == 'mock_endpoint_id'
