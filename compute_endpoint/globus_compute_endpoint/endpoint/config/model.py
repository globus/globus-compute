from __future__ import annotations

import pathlib
import typing as t
import uuid

from globus_compute_endpoint import engines
from globus_compute_endpoint.endpoint.config.pam import PamConfiguration
from globus_compute_endpoint.engines.base import GlobusComputeEngineBase
from parsl import addresses as parsl_addresses
from parsl import launchers as parsl_launchers
from parsl import providers as parsl_providers
from parsl.launchers.base import Launcher as ParslLauncherBase
from parsl.providers.base import ExecutionProvider as ParslProviderBase
from pydantic import (
    BaseModel,
    ConfigDict,
    FilePath,
    ValidateAs,
    model_validator,
)


def _check_import(module: t.ModuleType):
    def inner(obj_name: str):
        obj = getattr(module, obj_name, None)
        if obj is None:
            raise ValueError(f"{obj_name} could not be found")
        return obj

    return inner


def _materialize_model_from_type(model: t.Optional[BaseModel]):
    if not isinstance(model, BaseModel):
        return model

    fields = model.model_dump(exclude_none=True)
    _type = fields.pop("type")
    try:
        return _type(**fields)
    except Exception as err:
        raise ValueError(str(err)) from err


class BaseConfigModel(BaseModel):
    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)


class AddressModel(BaseConfigModel):
    type: t.Annotated[
        t.Callable[..., str],
        ValidateAs(str, _check_import(parsl_addresses)),
    ]


class LauncherModel(BaseConfigModel):
    type: t.Annotated[
        t.Type[ParslLauncherBase], ValidateAs(str, _check_import(parsl_launchers))
    ]


class ProviderModel(BaseConfigModel):
    type: t.Annotated[
        t.Type[ParslProviderBase], ValidateAs(str, _check_import(parsl_providers))
    ]
    launcher: t.Annotated[
        t.Optional[ParslLauncherBase],
        ValidateAs(t.Optional[LauncherModel], _materialize_model_from_type),
    ] = None
    persistent_volumes: t.Optional[t.List[t.Tuple[str, str]]] = (
        None  # KubernetesProvider
    )


class EngineModel(BaseConfigModel):
    type: t.Annotated[
        t.Type[GlobusComputeEngineBase], ValidateAs(str, _check_import(engines))
    ] = "GlobusComputeEngine"
    provider: t.Annotated[
        t.Optional[ParslProviderBase],
        ValidateAs(t.Optional[ProviderModel], _materialize_model_from_type),
    ] = None
    address: t.Annotated[
        t.Optional[str],
        ValidateAs(t.Optional[AddressModel | str], _materialize_model_from_type),
    ] = None
    strategy: t.Optional[str] = None
    worker_ports: t.Optional[t.Tuple[int, int]] = None
    worker_port_range: t.Optional[t.Tuple[int, int]] = None
    interchange_port_range: t.Optional[t.Tuple[int, int]] = None
    max_retries_on_system_failure: t.Optional[int] = None
    allowed_serializers: t.Optional[t.List[str]] = None

    model_config = ConfigDict(
        extra="allow", validate_default=True, arbitrary_types_allowed=True
    )

    @model_validator(mode="before")
    @classmethod
    def _validate_provider_container_compatibility(cls, values: dict):
        provider_type = values.get("provider", {}).get("type")
        if provider_type in (
            "AWSProvider",
            "GoogleCloudProvider",
            "KubernetesProvider",
        ):
            if values.get("container_uri"):
                raise ValueError(
                    f"The 'container_uri' field is not compatible with {provider_type}"
                    " because this provider manages containers internally. For more"
                    f" information on how to configure {provider_type}, please refer to"
                    f" Parsl documentation: https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.{provider_type}.html"  # noqa"
                )
        return values


class BaseEndpointConfigModel(BaseModel):
    display_name: t.Optional[str] = None
    allowed_functions: t.Optional[t.List[uuid.UUID]] = None
    admins: t.Optional[t.List[uuid.UUID]] = None
    authentication_policy: t.Optional[uuid.UUID] = None
    subscription_id: t.Optional[uuid.UUID] = None
    amqp_port: t.Optional[int] = None
    heartbeat_period: t.Optional[int] = None
    environment: t.Optional[str] = None
    local_compute_services: t.Optional[bool] = None
    high_assurance: t.Optional[bool] = None
    debug: t.Optional[bool] = None

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)


class UserEndpointConfigModel(BaseEndpointConfigModel):
    engine: t.Annotated[
        GlobusComputeEngineBase, ValidateAs(EngineModel, _materialize_model_from_type)
    ]
    heartbeat_threshold: t.Optional[int] = None
    idle_heartbeats_soft: t.Optional[int] = None
    idle_heartbeats_hard: t.Optional[int] = None
    endpoint_setup: t.Optional[str] = None
    endpoint_teardown: t.Optional[str] = None
    log_dir: t.Optional[str] = None
    stdout: t.Optional[str] = None
    stderr: t.Optional[str] = None


class ManagerEndpointConfigModel(BaseEndpointConfigModel):
    public: t.Optional[bool] = None
    user_config_template_path: t.Optional[FilePath] = None
    user_config_schema_path: t.Optional[FilePath] = None
    identity_mapping_config_path: t.Optional[FilePath] = None
    audit_log_path: t.Optional[FilePath] = None
    mu_child_ep_grace_period_s: t.Optional[float] = None
    pam: t.Optional[PamConfiguration] = None
