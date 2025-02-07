import pathlib
import typing as t
from unittest import mock

import pytest
from globus_compute_common.pydantic_v1 import ValidationError
from globus_compute_endpoint.endpoint.config import (
    ManagerEndpointConfig,
    ManagerEndpointConfigModel,
    PamConfiguration,
    UserEndpointConfig,
    UserEndpointConfigModel,
)
from globus_compute_endpoint.endpoint.config.model import EngineModel
from tests.unit.conftest import known_manager_config_opts, known_user_config_opts

_MOCK_BASE = "globus_compute_endpoint.endpoint.config."


@pytest.fixture
def config_dict():
    return {"engine": {"type": "GlobusComputeEngine", "address": "localhost"}}


@pytest.fixture
def config_dict_mu(fs):
    p = pathlib.Path("/some/dir")
    p.mkdir(parents=True)
    idc = p / "idconf.json"
    idc.write_text("[]")
    return {
        "identity_mapping_config_path": idc,
        "multi_user": True,
    }


@pytest.fixture
def mock_log():
    with mock.patch(f"{_MOCK_BASE}config.log") as m:
        yield m


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

    e_conf = config_dict["engine"]
    e_conf[field] = expected_val
    model = EngineModel(**e_conf)
    assert getattr(model, field) == expected_val

    e_conf[field] = list(expected_val)
    model = EngineModel(**e_conf)
    assert getattr(model, field) == expected_val

    e_conf[field] = 50000
    with pytest.raises(ValueError) as pyt_e:
        EngineModel(**e_conf)

    e_str = str(pyt_e)
    assert field in e_str, "Verify test; are we testing what we think?"
    assert "not a valid tuple" in e_str, "Verify test; are we testing what we think?"


def test_config_model_enforces_engine(config_dict):
    del config_dict["engine"]
    with pytest.raises(ValidationError) as pyt_exc:
        UserEndpointConfigModel(**config_dict)

    assert "engine\n  field required" in str(pyt_exc.value)


def test_mu_config_verifies_identity_mapping(config_dict_mu):
    conf_p = pathlib.Path("/some/path/not exists file")
    config_dict_mu["identity_mapping_config_path"] = conf_p
    with pytest.raises(ValidationError) as pyt_e:
        ManagerEndpointConfigModel(**config_dict_mu)

    e_str = str(pyt_e.value)
    assert "does not exist" in e_str
    assert str(conf_p) in e_str, "expect location in exc to help human out"

    del config_dict_mu["identity_mapping_config_path"]
    ManagerEndpointConfigModel(
        **config_dict_mu
    )  # doesn't raise; conditional validation


def test_mu_config_warns_idmapping_ignored(mock_log, config_dict_mu):
    config_dict_mu["identity_mapping_config_path"] = "not exists file"
    ManagerEndpointConfig(**config_dict_mu)

    a, _k = mock_log.warning.call_args
    assert "Identity mapping specified" in a[0]
    assert "is not privileged" in a[0]


def test_mu_config_privileged_requires_idmapping(config_dict_mu):
    del config_dict_mu["identity_mapping_config_path"]
    with mock.patch(f"{_MOCK_BASE}config.is_privileged", return_value=True):
        with pytest.raises(ValueError) as pyt_e:
            ManagerEndpointConfig(**config_dict_mu)

    assert "identity mapping" in str(pyt_e).lower()
    assert "required" in str(pyt_e).lower()
    assert "Hint: identity_mapping_config_path" in str(pyt_e), "Expect config item hint"


def test_mu_config_privileged_verifies_idmapping(config_dict_mu):
    p = config_dict_mu["identity_mapping_config_path"]
    with mock.patch(f"{_MOCK_BASE}config.is_privileged", return_value=True):
        ManagerEndpointConfig(**config_dict_mu)  # Verify for test: doesn't raise!

        p.unlink(missing_ok=True)
        with pytest.raises(ValueError) as pyt_e:
            ManagerEndpointConfig(**config_dict_mu)

        assert "not found" in str(pyt_e)
        assert str(p) in str(pyt_e), "Expect invalid path shared"


@pytest.mark.parametrize("public", (None, True, False, "a", 1))
def test_mu_public(public: t.Any):
    c = ManagerEndpointConfig(multi_user=True, public=public)
    assert c.public is (public is True)


@pytest.mark.parametrize(
    "provider_type, compatible",
    (
        ("LocalProvider", True),
        ("AWSProvider", False),
        ("GoogleCloudProvider", False),
        ("KubernetesProvider", False),
    ),
)
def test_provider_container_compatibility(
    config_dict: dict, provider_type: str, compatible: bool
):
    config_dict["engine"]["container_uri"] = "docker://ubuntu"
    config_dict["engine"]["provider"] = {"type": provider_type}
    config_dict["engine"]["address"] = "::1"

    if compatible:
        conf = UserEndpointConfigModel(**config_dict)
        conf.engine.shutdown()
    else:
        with pytest.raises(ValueError) as pyt_e:
            UserEndpointConfigModel(**config_dict)
        assert f"not compatible with {provider_type}" in str(pyt_e.value)


def test_configs_repr_default_kwargs():
    assert repr(UserEndpointConfig()) == "UserEndpointConfig()"
    defs = f"multi_user=True, pam={PamConfiguration(enable=False)!r}"
    assert (
        repr(ManagerEndpointConfig()) == f"ManagerEndpointConfig({defs})"
    ), "mu is on base"


@pytest.mark.parametrize("kw,cls", known_user_config_opts.items())
def test_userconfig_repr_nondefault_kwargs(
    randomstring, kw, cls, get_random_of_datatype
):
    if kw in ("engine", "executors"):
        return

    val = get_random_of_datatype(cls)
    kwds = {kw: val}

    repr_c = repr(UserEndpointConfig(**kwds))

    if kw in ["multi_user", "high_assurance"]:
        assert f"{kw}={repr(val)}" not in repr_c, "Multi-user and HA *off* by default"
    else:
        assert f"{kw}={repr(val)}" in repr_c


@pytest.mark.parametrize("kw,cls", known_manager_config_opts.items())
def test_managerconfig_repr_nondefault_kwargs(
    randomstring, fs, kw, cls, get_random_of_datatype
):
    val = get_random_of_datatype(cls)

    if kw == "identity_mapping_config_path":
        val = pathlib.Path(val)
        with mock.patch(f"{_MOCK_BASE}config.is_privileged", return_value=True):
            repr_c = repr(ManagerEndpointConfig(**{kw: val}))
    else:
        repr_c = repr(ManagerEndpointConfig(**{kw: val}))

    assert f"{kw}={repr(val)}" in repr_c


def test_engine_model_objects_allow_extra():
    config_dict = {
        "engine": {
            "type": "GlobusComputeEngine",
            "address": {
                "type": "address_by_interface",
                "ifname": "lo",  # Not specified in model
            },
            "provider": {
                "type": "LocalProvider",
                "max_blocks": 2,  # Not specified in model
            },
        }
    }
    UserEndpointConfigModel(**config_dict).engine.shutdown()
