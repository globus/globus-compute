import os
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
from globus_compute_endpoint.endpoint.config.model import EngineModel, ProviderModel
from tests.unit.conftest import known_manager_config_opts, known_user_config_opts

_MOCK_BASE = "globus_compute_endpoint.endpoint.config."


@pytest.fixture
def config_dict():
    return {"engine": {"type": "GlobusComputeEngine", "address": "localhost"}}


@pytest.fixture
def config_dict_mep(fs):
    p = pathlib.Path("/some/dir")
    p.mkdir(parents=True)
    idc = p / "idconf.json"
    idc.write_text("[]")
    return {"identity_mapping_config_path": idc}


@pytest.fixture
def mock_log():
    with mock.patch(f"{_MOCK_BASE}config.log") as m:
        yield m


@pytest.mark.parametrize(
    "field, expected_val",
    [
        ("worker_ports", (50000, 55000)),
        ("worker_port_range", (50000, 55000)),
        ("interchange_port_range", (50000, 55000)),
    ],
)
def test_config_engine_model_tuple_conversions(
    config_dict: dict, field: str, expected_val: t.Tuple
):
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


def test_config_provider_persistent_volumes_conversion():
    field = "persistent_volumes"
    expected_val = [("pvc1", "/path/to/dir"), ("pvc2", "/path/to/dir2")]
    p_conf = {"type": "KubernetesProvider", field: expected_val}

    model = ProviderModel(**p_conf)
    assert model.persistent_volumes == expected_val

    p_conf[field] = [list(t) for t in expected_val]
    model = ProviderModel(**p_conf)
    assert model.persistent_volumes == expected_val

    p_conf[field] = [t[0] for t in expected_val]
    with pytest.raises(ValueError) as pyt_e:
        ProviderModel(**p_conf)

    p_str = str(pyt_e)
    assert field in p_str, "Verify test; are we testing what we think?"
    assert "not a valid tuple" in p_str, "Verify test; are we testing what we think?"


def test_config_model_enforces_engine(config_dict):
    del config_dict["engine"]
    with pytest.raises(ValidationError) as pyt_exc:
        UserEndpointConfigModel(**config_dict)

    assert "engine\n  field required" in str(pyt_exc.value)


def test_manager_config_model_rejects_engine(config_dict_mep):
    config_dict_mep["engine"] = {
        "type": "GlobusComputeEngine",
        "address": "localhost",
    }
    with pytest.raises(ValidationError) as pyt_exc:
        ManagerEndpointConfigModel(**config_dict_mep)

    assert "engine\n  extra fields not permitted" in str(pyt_exc.value)


@pytest.mark.parametrize(
    "field",
    (
        "identity_mapping_config_path",
        "user_config_template_path",
        "user_config_schema_path",
    ),
)
def test_mep_config_verifies_path_like_fields(config_dict_mep, field: str):
    conf_p = pathlib.Path("/some/path/not exists file")
    config_dict_mep[field] = conf_p
    with pytest.raises(ValidationError) as pyt_e:
        ManagerEndpointConfigModel(**config_dict_mep)

    e_str = str(pyt_e.value)
    assert "does not exist" in e_str
    assert str(conf_p) in e_str, "expect location in exc to help human out"

    del config_dict_mep[field]
    ManagerEndpointConfigModel(
        **config_dict_mep
    )  # doesn't raise; conditional validation


def test_mep_config_privileged_verifies_idmapping(config_dict_mep):
    p = config_dict_mep["identity_mapping_config_path"]
    ManagerEndpointConfig(**config_dict_mep)  # Verify for test: doesn't raise!

    p.unlink(missing_ok=True)
    with pytest.raises(ValueError) as pyt_e:
        ManagerEndpointConfig(**config_dict_mep)

    assert "not found" in str(pyt_e)
    assert str(p) in str(pyt_e), "Expect invalid path shared"


@pytest.mark.parametrize("public", (None, True, False, "a", 1))
def test_mep_public(public: t.Any):
    c = ManagerEndpointConfig(public=public)
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
    defs = f"pam={PamConfiguration(enable=False)!r}"
    assert (
        repr(ManagerEndpointConfig()) == f"ManagerEndpointConfig({defs})"
    ), "mep is on base"


@pytest.mark.parametrize("kw,cls", known_user_config_opts.items())
def test_userconfig_repr_nondefault_kwargs(
    randomstring, kw, cls, get_random_of_datatype
):
    if kw in ("engine", "detach_endpoint"):
        return

    val = get_random_of_datatype(cls)
    kwds = {kw: val}

    repr_c = repr(UserEndpointConfig(**kwds))

    if kw == "high_assurance":
        assert f"{kw}={repr(val)}" not in repr_c, "HA *off* by default"
    else:
        assert f"{kw}={repr(val)}" in repr_c


@pytest.mark.parametrize("kw,cls", known_manager_config_opts.items())
def test_managerconfig_repr_nondefault_kwargs(
    randomstring, fs, kw, cls, get_random_of_datatype
):
    val = get_random_of_datatype(cls)
    if cls == os.PathLike:
        val = pathlib.Path(val)

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


@pytest.mark.parametrize("multi_user", (True, False))
@pytest.mark.parametrize("config_class", (UserEndpointConfig, ManagerEndpointConfig))
def test_multi_user_deprecated(multi_user, config_class):
    with pytest.deprecated_call() as pyt_warns:
        config_class(multi_user=multi_user)

    assert "multi_user" in str(pyt_warns.list[0].message)


@pytest.mark.parametrize("detach_endpoint", (True, False))
def test_detach_endpoint_deprecated(detach_endpoint):
    with pytest.deprecated_call() as pyt_warns:
        UserEndpointConfig(detach_endpoint=detach_endpoint)

    assert "detach_endpoint" in str(pyt_warns.list[0].message)
