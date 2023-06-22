from __future__ import annotations

import typing as t
from types import ModuleType

from globus_compute_endpoint import strategies
from globus_compute_endpoint.executors import HighThroughputExecutor
from parsl import addresses as parsl_addresses
from parsl import channels as parsl_channels
from parsl import launchers as parsl_launchers
from parsl import providers as parsl_providers
from pydantic import BaseModel, Field, validator


def _validate_import(field: str, package: ModuleType):
    def inner(cls, module: str):
        cls = getattr(package, module, None)
        if cls is None:
            raise ValueError(f"{module} could not be found")
        return cls

    return validator(field, allow_reuse=True)(inner)


def _validate_params(field: str):
    def inner(cls, model: t.Optional[BaseModel]):
        if not isinstance(model, BaseModel):
            return model

        fields = model.dict(exclude_none=True)
        cls = fields.pop("type")
        try:
            return cls(**fields)
        except Exception as err:
            raise ValueError(str(err)) from err

    return validator(field, allow_reuse=True)(inner)


class AddressModel(BaseModel):
    type: str

    _validate_type = _validate_import("type", parsl_addresses)

    class Config:
        extra = "allow"


class StrategyModel(BaseModel):
    type: str

    _validate_type = _validate_import("type", strategies)

    class Config:
        extra = "allow"


class LauncherModel(BaseModel):
    type: str

    _validate_type = _validate_import("type", parsl_launchers)

    class Config:
        extra = "allow"


class ChannelModel(BaseModel):
    type: str

    _validate_type = _validate_import("type", parsl_channels)

    class Config:
        extra = "allow"


class ProviderModel(BaseModel):
    type: str
    channel: t.Optional[ChannelModel]
    launcher: t.Optional[LauncherModel]

    _validate_type = _validate_import("type", parsl_providers)
    _validate_channel = _validate_params("channel")
    _validate_launcher = _validate_params("launcher")

    class Config:
        extra = "allow"


class EngineModel(BaseModel):
    type: type = Field(default=HighThroughputExecutor, const=True)
    provider: ProviderModel
    strategy: t.Optional[StrategyModel]
    address: t.Optional[t.Union[str, AddressModel]]

    _validate_provider = _validate_params("provider")
    _validate_strategy = _validate_params("strategy")
    _validate_address = _validate_params("address")

    class Config:
        extra = "allow"


class ConfigModel(BaseModel):
    engine: EngineModel
    display_name: t.Optional[str]
    environment: t.Optional[str]
    funcx_service_address: t.Optional[str]
    multi_tenant: t.Optional[bool]
    allowed_functions: t.Optional[t.List[str]]
    heartbeat_period: t.Optional[int]
    heartbeat_threshold: t.Optional[int]
    idle_heartbeats_soft: t.Optional[int]
    idle_heartbeats_hard: t.Optional[int]
    detach_endpoint: t.Optional[bool]
    log_dir: t.Optional[str]
    stdout: t.Optional[str]
    stderr: t.Optional[str]

    _validate_engine = _validate_params("engine")

    def dict(self, *args, **kwargs):
        # Slight modification is needed here since we still
        # store the engine/executor in a list named executors
        ret = super().dict(*args, **kwargs)
        executor = ret.pop("engine")
        ret["executors"] = [executor]
        return ret

    class Config:
        extra = "allow"
