"""
Requires pytest

pip install pytest

pytest test_throttling.py
"""

import pytest
import globus_sdk
from unittest.mock import Mock

from funcx.sdk.utils.throttling import (ThrottledBaseClient,
                                        MaxRequestSizeExceeded,
                                        MaxRequestsExceeded)


@pytest.fixture
def mock_globus_sdk(monkeypatch):
    monkeypatch.setattr(globus_sdk.base.BaseClient, '__init__', Mock())


def test_size_throttling_on_small_requests(mock_globus_sdk):
    cli = ThrottledBaseClient()

    # Should not raise
    jb = {'not': 'big enough'}
    cli.throttle_request_size('POST', '/my_rest_endpoint', json_body=jb)

    # Should not raise for these methods
    cli.throttle_request_size('GET', '/my_rest_endpoint')
    cli.throttle_request_size('PUT', '/my_rest_endpoint')
    cli.throttle_request_size('DELETE', '/my_rest_endpoint')


def test_size_throttle_on_large_request(mock_globus_sdk):
    cli = ThrottledBaseClient()
    # Test with ~2mb sized POST
    jb = {'is': 'l' + 'o' * 2 * 2 ** 20 + 'ng'}
    with pytest.raises(MaxRequestSizeExceeded):
        cli.throttle_request_size('POST', '/my_rest_endpoint', json_body=jb)

    # Test on text request
    data = 'B' + 'i' * 2 * 2 ** 20 + 'gly'
    with pytest.raises(MaxRequestSizeExceeded):
        cli.throttle_request_size('POST', '/my_rest_endpoint', text_body=data)


def test_low_threshold_requests_does_not_raise(mock_globus_sdk):
    cli = ThrottledBaseClient()
    for _ in range(4):
        cli.throttle_max_requests()


def test_max_requests_raises_exception(mock_globus_sdk):
    cli = ThrottledBaseClient()
    with pytest.raises(MaxRequestsExceeded):
        for _ in range(10):
            cli.throttle_max_requests()
