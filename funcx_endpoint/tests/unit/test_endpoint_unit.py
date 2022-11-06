import functools
import io
import random
import uuid
from collections import namedtuple
from unittest import mock

import pytest

from funcx_endpoint.endpoint.endpoint import Endpoint


@pytest.fixture
def mock_ep():
    buf = io.StringIO()
    ep = Endpoint()
    ep.get_endpoints = mock.Mock()
    ep.get_endpoints.return_value = {}
    ep.print_endpoint_table = functools.partial(ep.print_endpoint_table, ofile=buf)
    yield ep, buf


def test_list_endpoints_none_configured(mock_ep):
    ep, buf = mock_ep
    ep.print_endpoint_table()
    assert "No endpoints configured" in buf.getvalue()
    assert "Hint:" in buf.getvalue()
    assert "funcx-endpoint configure" in buf.getvalue()


def test_list_endpoints_no_id_yet(mock_ep, randomstring):
    ep, buf = mock_ep
    expected_col_length = random.randint(2, 30)
    ep.get_endpoints.return_value = {
        "default": {"status": randomstring(length=expected_col_length), "id": None}
    }
    ep.print_endpoint_table()
    assert ep.get_endpoints.return_value["default"]["status"] in buf.getvalue()
    assert "| Endpoint ID |" in buf.getvalue(), "Expecting column shrinks to size"


@pytest.mark.parametrize("term_size", ((30, 5), (50, 5), (67, 5), (72, 5), (120, 5)))
def test_list_endpoints_long_names_wrapped(mock_ep, mocker, term_size, randomstring):
    ep, buf = mock_ep
    tsize = namedtuple("terminal_size", ["columns", "lines"])(*term_size)
    mock_shutil = mocker.patch("funcx_endpoint.endpoint.endpoint.shutil")
    mock_shutil.get_terminal_size.return_value = tsize

    def rand_length_str(min_=2, max_=30):
        return randomstring(length=random.randint(min_, max_))

    expected_data = {
        rand_length_str(100, 110): {"status": rand_length_str(), "id": uuid.uuid4()},
        rand_length_str(100, 110): {"status": rand_length_str(), "id": uuid.uuid4()},
        rand_length_str(100, 110): {"status": rand_length_str(), "id": uuid.uuid4()},
        rand_length_str(100, 110): {"status": rand_length_str(), "id": None},
        rand_length_str(100, 110): {"status": rand_length_str(), "id": uuid.uuid4()},
    }
    ep.get_endpoints.return_value = expected_data

    ep.print_endpoint_table()

    for ep_name, ep in expected_data.items():
        assert ep["status"] in buf.getvalue(), "expected no wrapping of status"
        assert str(ep["id"]) in buf.getvalue(), "expected no wrapping of id"
        assert ep_name not in buf.getvalue(), "expected only name column is wrapped"
