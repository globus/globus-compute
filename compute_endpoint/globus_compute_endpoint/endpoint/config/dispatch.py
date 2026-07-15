import abc
import types
import typing as t

import globus_compute_endpoint.engines
import parsl.addresses
import parsl.launchers
import parsl.providers
from globus_compute_endpoint.engines.base import GlobusComputeEngineBase
from parsl.launchers.base import Launcher
from parsl.providers.base import ExecutionProvider
from pydantic import ConfigDict, validate_call, with_config
from typing_extensions import TypedDict


@with_config(ConfigDict(extra="allow", arbitrary_types_allowed=True))
class TypeTaggedDict(TypedDict):
    type: str


T = t.TypeVar("T")


class TypeDispatcher(abc.ABC, t.Generic[T]):
    _source_module: types.ModuleType
    _name: str
    _children: dict[str, type["TypeDispatcher[t.Any]"]] = {}

    @classmethod
    @validate_call
    def build_instance(cls, input: TypeTaggedDict) -> T:
        data: dict[str, t.Any] = dict(input)
        for field_name, field_type in cls._children.items():
            if field_name in data:
                data[field_name] = field_type.build_instance(data[field_name])

        type_name = data.pop("type")
        obj = getattr(cls._source_module, type_name, None)
        if obj is None:
            valid_options = "\n\t".join(
                k for k in dir(cls._source_module) if not k.startswith("_")
            )
            raise ValueError(
                f"'{type_name}' is not a valid {cls._name}. Valid options are: "
                f"{valid_options}"
            )

        try:
            # exclude None values to match the old Pydantic v1 `exclude_none=True` behavior
            instance = obj(**{k: v for k, v in data.items() if v is not None})
        except Exception as e:
            raise ValueError(str(e)) from e
        return t.cast(T, instance)


class AddressDispatcher(TypeDispatcher[str]):
    _source_module = parsl.addresses
    _name = "address"

    @classmethod
    @validate_call
    def build_instance(cls, input: TypeTaggedDict | str) -> str:
        if isinstance(input, str):
            return input
        return super().build_instance(input)


class LauncherDispatcher(TypeDispatcher[Launcher]):
    _source_module = parsl.launchers
    _name = "launcher"


class ProviderDispatcher(TypeDispatcher[ExecutionProvider]):
    _source_module = parsl.providers
    _name = "provider"
    _children = {
        "launcher": LauncherDispatcher,
    }


class EngineDispatcher(TypeDispatcher[GlobusComputeEngineBase]):
    _source_module = globus_compute_endpoint.engines
    _name = "engine"
    _children = {
        "provider": ProviderDispatcher,
        "address": AddressDispatcher,
    }

    @classmethod
    @validate_call
    def build_instance(cls, input: TypeTaggedDict) -> GlobusComputeEngineBase:
        data: dict[str, t.Any] = dict(input)
        provider_type = data.get("provider", {}).get("type")
        if provider_type in (
            "AWSProvider",
            "GoogleCloudProvider",
            "KubernetesProvider",
        ):
            if data.get("container_uri"):
                raise ValueError(
                    f"The 'container_uri' field is not compatible with {provider_type}"
                    " because this provider manages containers internally. For more"
                    f" information on how to configure {provider_type}, please refer to"
                    f" Parsl documentation: https://parsl.readthedocs.io/en/stable/stubs/parsl.providers.{provider_type}.html"  # noqa
                )
        return super().build_instance(input)
