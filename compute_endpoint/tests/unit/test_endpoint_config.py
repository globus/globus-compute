import typing as t

import pytest
from globus_compute_endpoint.endpoint.config.model import ConfigModel


@pytest.fixture
def config_dict():
    return {"engine": {"type": "HighThroughputEngine"}}


@pytest.mark.parametrize(
    "data",
    [
        ("worker_ports", (50000, 55000)),
        ("worker_port_range", (50000, 55000)),
        ("interchange_port_range", (50000, 55000)),
    ],
)
def test_config_model_tuple_conversions(config_dict: dict, data: t.Tuple[str, t.Tuple]):
    field, expected_val = data

    config_dict["engine"][field] = expected_val
    model = ConfigModel(**config_dict)
    assert getattr(model.engine, field) == expected_val

    config_dict["engine"][field] = list(expected_val)
    model = ConfigModel(**config_dict)
    assert getattr(model.engine, field) == expected_val

    config_dict["engine"][field] = 50000
    with pytest.raises(ValueError):
        ConfigModel(**config_dict)
