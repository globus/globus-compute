import copy
import os
import pathlib
import types
import typing as t
from unittest import mock

import pytest
from globus_compute_endpoint.endpoint.config import (
    BaseConfig,
    ManagerEndpointConfig,
    PamConfiguration,
    PathConfiguration,
    UserEndpointConfig,
)
from globus_compute_endpoint.endpoint.config.dispatch import (
    AddressDispatcher,
    EngineDispatcher,
    LauncherDispatcher,
    ProviderDispatcher,
)
from pydantic import TypeAdapter, ValidationError
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
    with pytest.raises(ValueError) as pyt_e:
        ManagerEndpointConfig(**config_dict_mep)

    e_str = str(pyt_e.value)
    assert "not found" in e_str
    assert str(conf_p) in e_str, "expect location in exc to help human out"

    del config_dict_mep[field]
    ManagerEndpointConfig(**config_dict_mep)  # doesn't raise; conditional validation


def test_mep_config_privileged_verifies_idmapping(config_dict_mep):
    p = config_dict_mep["identity_mapping_config_path"]
    ManagerEndpointConfig(**config_dict_mep)  # Verify for test: doesn't raise!

    p.unlink(missing_ok=True)
    with pytest.raises(ValueError) as pyt_e:
        ManagerEndpointConfig(**config_dict_mep)

    assert "not found" in str(pyt_e)
    assert str(p) in str(pyt_e), "Expect invalid path shared"


ta_bool = TypeAdapter(bool)


@pytest.mark.parametrize("public", (None, True, False, "a", 1))
def test_mep_public(public: t.Any):
    try:
        ta_bool.validate_python(public)
    except ValidationError:
        with pytest.raises(ValidationError):
            ManagerEndpointConfig(public=public)
    else:
        c = ManagerEndpointConfig(public=public)
        assert c.public is bool(public), "Verify that public is set to what we expect"


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
    engine_dict = config_dict["engine"]
    engine_dict["container_uri"] = "docker://ubuntu"
    engine_dict["provider"] = {"type": provider_type}
    engine_dict["address"] = "::1"

    if compatible:
        EngineDispatcher.build_instance(engine_dict).shutdown()
    else:
        with pytest.raises(ValueError) as pyt_e:
            EngineDispatcher.build_instance(engine_dict)
        assert f"not compatible with {provider_type}" in str(pyt_e.value)


def test_configs_repr_default_kwargs():
    assert repr(UserEndpointConfig()) == "UserEndpointConfig()"
    defs = f"pam={PamConfiguration(enable=False)!r}"
    assert repr(ManagerEndpointConfig()) == f"ManagerEndpointConfig({defs})", (
        "mep is on base"
    )


@pytest.mark.parametrize("kw,cls", known_user_config_opts.items())
def test_userconfig_repr_nondefault_kwargs(
    randomstring, kw, cls, get_random_of_datatype
):
    # Skip optional sections
    if kw == "engine":
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


def test_engine_dispatcher_objects_allow_extra():
    engine_dict = {
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
    EngineDispatcher.build_instance(engine_dict).shutdown()


def test_dispatcher_requires_type_field():
    with pytest.raises(ValidationError) as pyt_e:
        EngineDispatcher.build_instance({})

    assert "type" in str(pyt_e.value)


@pytest.mark.parametrize(
    "dispatcher, cfg, expected",
    (
        (EngineDispatcher, {"type": "NoSuchEngine"}, "not a valid engine"),
        (ProviderDispatcher, {"type": "NoSuchProvider"}, "not a valid provider"),
        (LauncherDispatcher, {"type": "NoSuchLauncher"}, "not a valid launcher"),
        (AddressDispatcher, {"type": "NoSuchAddress"}, "not a valid address"),
        (
            ProviderDispatcher,
            {"type": "LocalProvider", "launcer": {"type": "SingleNodeLauncher"}},
            "unexpected keyword argument",
        ),
        (
            LauncherDispatcher,
            {"type": "SingleNodeLauncher", "debgu": True},
            "unexpected keyword argument",
        ),
    ),
)
def test_dispatcher_rejects_bad_input(dispatcher, cfg, expected):
    with pytest.raises(ValueError) as pyt_e:
        dispatcher.build_instance(cfg)

    assert expected in str(pyt_e.value)


def test_address_dispatcher_accepts_string_or_typed_dict(monkeypatch):
    mock_addresses = types.SimpleNamespace(UnitTestAddress=lambda: "test-addr")
    monkeypatch.setattr(AddressDispatcher, "_source_module", mock_addresses)

    assert AddressDispatcher.build_instance("127.0.0.1") == "127.0.0.1"
    assert AddressDispatcher.build_instance({"type": "UnitTestAddress"}) == "test-addr"


def test_dispatcher_does_not_mutate_input_dict():
    provider_cfg = {
        "type": "LocalProvider",
        "launcher": {"type": "SingleNodeLauncher"},
    }
    provider_cfg_orig = copy.deepcopy(provider_cfg)

    ProviderDispatcher.build_instance(provider_cfg)

    assert provider_cfg == provider_cfg_orig


@pytest.mark.parametrize(
    "cls",
    (
        BaseConfig,
        UserEndpointConfig,
        ManagerEndpointConfig,
    ),
)
def test_config_init_validated_with_pydantic(cls):
    with pytest.raises(ValidationError):
        cls(heartbeat_period=object())
