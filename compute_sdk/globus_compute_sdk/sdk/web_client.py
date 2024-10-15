"""
This module implements a Globus SDK client class suitable for use with the
Globus Compute web service.

It also implements data helpers for building complex payloads. Most notably,
`FunctionRegistrationData` which can be constructed from an arbitrary callable.
"""

import json
import typing as t
import warnings

import globus_sdk
from globus_compute_common.sdk_version_sharing import user_agent_substring
from globus_compute_sdk.sdk._environments import get_web_service_url, remove_url_path
from globus_compute_sdk.sdk.utils.uuid_like import UUID_LIKE_T
from globus_compute_sdk.serialize import ComputeSerializer
from globus_compute_sdk.version import __version__
from globus_sdk import GlobusAPIError, GlobusApp, Scope

from .auth.scopes import ComputeScopes


def _get_packed_code(
    func: t.Callable, serializer: t.Optional[ComputeSerializer] = None
) -> str:
    serializer = serializer if serializer else ComputeSerializer()
    return serializer.pack_buffers([serializer.serialize(func)])


class FunctionRegistrationMetadata:
    def __init__(self, python_version: str, sdk_version: str):
        self.python_version = python_version
        self.sdk_version = sdk_version

    def to_dict(self):
        return {
            "python_version": self.python_version,
            "sdk_version": self.sdk_version,
        }


class FunctionRegistrationData:
    def __init__(
        self,
        *,
        function: t.Optional[t.Callable] = None,
        function_name: t.Optional[str] = None,
        function_code: t.Optional[str] = None,
        container_uuid: t.Optional[UUID_LIKE_T] = None,
        description: t.Optional[str] = None,
        metadata: t.Optional[FunctionRegistrationMetadata] = None,
        public: bool = False,
        group: t.Optional[str] = None,
        serializer: t.Optional[ComputeSerializer] = None,
    ):
        if function is not None:
            function_name = function.__name__
            function_code = _get_packed_code(function, serializer=serializer)

        if function_name is None or function_code is None:
            raise ValueError(
                "Either 'function' must be provided, or "
                "both of 'function_name' and 'function_code'"
            )

        self.function_name = function_name
        self.function_code = function_code
        self.container_uuid = (
            str(container_uuid) if container_uuid is not None else container_uuid
        )
        self.description = description
        self.metadata = metadata
        self.public = public
        self.group = group

    def to_dict(self):
        return {
            "function_name": self.function_name,
            "function_code": self.function_code,
            "container_uuid": self.container_uuid,
            "description": self.description,
            "metadata": self.metadata.to_dict() if self.metadata else None,
            "public": self.public,
            "group": self.group,
        }

    def __str__(self):
        return "FunctionRegistrationData(" + json.dumps(self.to_dict()) + ")"


class WebClient(globus_sdk.BaseClient):
    # the `service_name` is used in the Globus SDK to lookup the service URL from
    # config. However, Globus Compute has its own logic for determining the base URL.
    # set `service_name` to allow the check which ensures this is set to pass
    # it does not have any other effects
    service_name: str = "funcx"
    # use the Globus Compute-specific error class
    error_class = GlobusAPIError

    scopes = ComputeScopes
    default_scope_requirements = [Scope(ComputeScopes.all)]

    def __init__(
        self,
        *,
        environment: t.Optional[str] = None,
        base_url: t.Optional[str] = None,
        app: t.Optional[GlobusApp] = None,
        app_name: t.Optional[str] = None,
        **kwargs,
    ):
        if base_url is None:
            base_url = get_web_service_url(environment)
        base_url = remove_url_path(base_url)

        if app_name is None:
            app_name = user_agent_substring(__version__)

        super().__init__(
            environment=environment,
            base_url=base_url,
            app=app,
            app_name=app_name,
            **kwargs,
        )

    @property
    def user_app_name(self) -> t.Optional[str]:
        warnings.warn(
            "'user_app_name' is deprecated. Please directly use 'app_name' instead.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        return self.app_name

    @user_app_name.setter
    def user_app_name(self, value: str):
        warnings.warn(
            "'user_app_name' is deprecated. Please directly use 'app_name' instead.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        if value is not None:
            self.app_name = f"{self.app_name}/{value}"

    def get_version(self, *, service: str = "all") -> globus_sdk.GlobusHTTPResponse:
        return self.get("/v2/version", query_params={"service": service})

    def get_taskgroup_tasks(
        self, task_group_id: UUID_LIKE_T
    ) -> globus_sdk.GlobusHTTPResponse:
        return self.get(f"/v2/taskgroup/{task_group_id}")

    def get_task(self, task_id: UUID_LIKE_T) -> globus_sdk.GlobusHTTPResponse:
        return self.get(f"/v2/tasks/{task_id}")

    def get_batch_status(
        self,
        task_ids: t.Iterable[UUID_LIKE_T],
        *,
        additional_fields: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> globus_sdk.GlobusHTTPResponse:
        if isinstance(task_ids, str):
            task_ids = [task_ids]
        data = {"task_ids": [str(t) for t in task_ids]}
        if additional_fields is not None:
            data.update(additional_fields)
        return self.post("/v2/batch_status", data=data)

    def submit(
        self, endpoint_id: UUID_LIKE_T, batch: t.Dict[str, t.Any]
    ) -> globus_sdk.GlobusHTTPResponse:
        return self.post(f"/v3/endpoints/{endpoint_id}/submit", data=batch)

    def register_endpoint(
        self,
        endpoint_name: str,
        endpoint_id: t.Optional[UUID_LIKE_T] = None,
        *,
        metadata: t.Optional[dict] = None,
        multi_user: t.Optional[bool] = None,
        display_name: t.Optional[str] = None,
        allowed_functions: t.Optional[t.List[UUID_LIKE_T]] = None,
        auth_policy: t.Optional[UUID_LIKE_T] = None,
        subscription_id: t.Optional[UUID_LIKE_T] = None,
        public: t.Optional[bool] = None,
        additional_fields: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> globus_sdk.GlobusHTTPResponse:
        data: t.Dict[str, t.Any] = {"endpoint_name": endpoint_name}

        # Only populate if not None.  "" is valid and will be included
        # No value or a 'None' on an existing endpoint will leave
        # the old display_name unchanged
        if display_name is not None:
            data["display_name"] = display_name

        # Only send this param if True.  Will have to change to
        # `if multi_user is not None` if we want to always pass it
        if multi_user:
            data["multi_user"] = multi_user

        if metadata:
            data["metadata"] = metadata
        if allowed_functions:
            data["allowed_functions"] = allowed_functions
        if auth_policy:
            data["authentication_policy"] = auth_policy
        if subscription_id:
            data["subscription_uuid"] = subscription_id
        if public is not None:
            data["public"] = public
        if additional_fields is not None:
            data.update(additional_fields)

        if endpoint_id:
            return self.put(f"/v3/endpoints/{endpoint_id}", data=data)
        else:
            return self.post("/v3/endpoints", data=data)

    def get_result_amqp_url(self) -> globus_sdk.GlobusHTTPResponse:
        return self.get("/v2/get_amqp_result_connection_url")

    def get_endpoint_status(
        self, endpoint_id: UUID_LIKE_T
    ) -> globus_sdk.GlobusHTTPResponse:
        return self.get(f"/v2/endpoints/{endpoint_id}/status")

    def get_endpoint_metadata(
        self, endpoint_id: UUID_LIKE_T
    ) -> globus_sdk.GlobusHTTPResponse:
        return self.get(f"/v2/endpoints/{endpoint_id}")

    def get_endpoints(self) -> globus_sdk.GlobusHTTPResponse:
        return self.get("/v2/endpoints")

    def register_function(
        self,
        function_registration_data: t.Union[
            t.Dict[str, t.Any], FunctionRegistrationData
        ],
    ) -> globus_sdk.GlobusHTTPResponse:
        data = (
            function_registration_data.to_dict()
            if isinstance(function_registration_data, FunctionRegistrationData)
            else function_registration_data
        )
        return self.post("/v2/functions", data=data)

    def get_function(self, function_id: UUID_LIKE_T) -> globus_sdk.GlobusHTTPResponse:
        return self.get(f"/v2/functions/{function_id}")

    def get_allowed_functions(
        self, endpoint_id: UUID_LIKE_T
    ) -> globus_sdk.GlobusHTTPResponse:
        return self.get(f"/v3/endpoints/{endpoint_id}/allowed_functions")

    def stop_endpoint(self, endpoint_id: UUID_LIKE_T) -> globus_sdk.GlobusHTTPResponse:
        return self.post(f"/v2/endpoints/{endpoint_id}/lock", data={})

    def delete_endpoint(
        self, endpoint_id: UUID_LIKE_T
    ) -> globus_sdk.GlobusHTTPResponse:
        return self.delete(f"/v2/endpoints/{endpoint_id}")

    def delete_function(
        self, function_id: UUID_LIKE_T
    ) -> globus_sdk.GlobusHTTPResponse:
        return self.delete(f"/v2/functions/{function_id}")
