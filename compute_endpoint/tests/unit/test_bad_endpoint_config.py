from unittest import mock

import pytest
from globus_compute_endpoint.engines import HighThroughputEngine

_MOCK_BASE = "globus_compute_endpoint.engines.high_throughput.engine."


@pytest.mark.parametrize("address", ("localhost", "login1.theta.alcf.anl.gov", "*"))
def test_invalid_address(address):
    with mock.patch(f"{_MOCK_BASE}log") as mock_log:
        with pytest.raises(ValueError):
            HighThroughputEngine(address=address)
    assert mock_log.critical.called


@pytest.mark.parametrize(
    "address", ("192.168.64.12", "fe80::e643:4bff:fe61:8f72", "129.114.44.12")
)
def test_valid_address(address):
    HighThroughputEngine(address=address)
