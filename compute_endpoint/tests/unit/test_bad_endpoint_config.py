import pytest
from globus_compute_endpoint.executors.high_throughput.executor import (
    HighThroughputExecutor,
)

invalid_addresses = ["localhost", "login1.theta.alcf.anl.gov", "*"]


@pytest.mark.parametrize("address", invalid_addresses)
def test_invalid_address(address):
    with pytest.raises(ValueError):
        HighThroughputExecutor(address=address)


valid_addresses = ["192.168.64.12", "fe80::e643:4bff:fe61:8f72", "129.114.44.12"]


@pytest.mark.parametrize("address", valid_addresses)
def test_valid_address(address):
    HighThroughputExecutor(address=address)
