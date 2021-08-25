from funcx_endpoint.endpoint.register_endpoint import register_endpoint
import os
import logging
import sys
import shutil
import pytest
import json
from pytest import fixture
from unittest.mock import ANY

logger = logging.getLogger('mock_funcx')


class TestRegisterEndpoint:

    @pytest.fixture(autouse=True)
    def test_setup_teardown(self):
        # Code that will run before your test, for example:

        funcx_dir = f'{os.getcwd()}'
        config_dir = os.path.join(funcx_dir, "mock_endpoint")
        assert not os.path.exists(config_dir)
        # A test function will be run at this point
        yield
        # Code that will run after your test, for example:
        if os.path.exists(config_dir):
            shutil.rmtree(config_dir)

    def test_register_endpoint_no_endpoint_id(self, mocker):
        mock_client = mocker.patch("funcx_endpoint.endpoint.endpoint_manager.FuncXClient")
        mock_client.return_value.register_endpoint.return_value = {'status': 'okay'}

        funcx_dir = os.getcwd()
        config_dir = os.path.join(funcx_dir, "mock_endpoint")

        with pytest.raises(Exception, match='Endpoint ID was not included in the service\'s registration response.'):
            register_endpoint(logger, mock_client(), 'mock_endpoint_uuid', config_dir, 'test')

    def test_register_endpoint_int_endpoint_id(self, mocker):
        mock_client = mocker.patch("funcx_endpoint.endpoint.endpoint_manager.FuncXClient")
        mock_client.return_value.register_endpoint.return_value = {'status': 'okay',
                                                                   'endpoint_id': 123456}

        funcx_dir = os.getcwd()
        config_dir = os.path.join(funcx_dir, "mock_endpoint")

        with pytest.raises(Exception, match='Endpoint ID sent by the service was not a string.'):
            register_endpoint(logger, mock_client(), 'mock_endpoint_uuid', config_dir, 'test')
