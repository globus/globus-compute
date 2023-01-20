"""
This module implements a Globus SDK client class suitable for use with the FuncX web
service.

It also implements data helpers for building complex payloads. Most notably,
`FunctionRegistrationData` which can be constructed from an arbitrary callable.
"""
import inspect
import json
import typing as t
import uuid

import globus_sdk
from funcx_common.sdk_version_sharing import user_agent_substring
from globus_sdk.exc.api import GlobusAPIError

from funcx.sdk._environments import get_web_service_url
from funcx.serialize import FuncXSerializer
from funcx.version import __version__

ID_PARAM_T = t.Union[uuid.UUID, str]


def _get_packed_code(
    func: t.Callable, serializer: t.Optional[FuncXSerializer] = None
) -> str:
    serializer = serializer if serializer else FuncXSerializer()
    return serializer.pack_buffers([serializer.serialize(func)])


class FunctionRegistrationData:
    def __init__(
        self,
        *,
        function: t.Optional[t.Callable] = None,
        function_name: t.Optional[str] = None,
        function_code: t.Optional[str] = None,
        function_source: t.Optional[str] = None,
        failover_source: t.Optional[str] = None,
        container_uuid: t.Optional[ID_PARAM_T] = None,
        entry_point: t.Optional[str] = None,
        description: t.Optional[str] = None,
        public: bool = False,
        group: t.Optional[str] = None,
        searchable: bool = True,
        serializer: t.Optional[FuncXSerializer] = None,
    ):
        if function is not None:
            function_name = function.__name__
            function_code = _get_packed_code(function, serializer=serializer)

            if function_source is None:
                try:
                    function_source = inspect.getsource(function)
                except OSError:
                    if failover_source is not None:
                        function_source = failover_source
                    else:
                        raise

        if function_name is None or function_code is None:
            raise ValueError(
                "Either 'function' must be provided, or "
                "both of 'function_name' and 'function_code'"
            )

        self.function_name = function_name
        self.function_code = function_code
        self.function_source = function_source
        self.container_uuid = (
            str(container_uuid) if container_uuid is not None else container_uuid
        )
        self.entry_point = entry_point if entry_point is not None else function_name
        self.description = description
        self.public = public
        self.group = group
        self.searchable = searchable

    def to_dict(self):
        return {
            "function_name": self.function_name,
            "function_code": self.function_code,
            "function_source": self.function_source,
            "container_uuid": self.container_uuid,
            "entry_point": self.entry_point,
            "description": self.description,
            "public": self.public,
            "group": self.group,
            "searchable": self.searchable,
        }

    def __str__(self):
        return "FunctionRegistrationData(" + json.dumps(self.to_dict()) + ")"


class FuncxWebClient(globus_sdk.BaseClient):
    # the `service_name` is used in the Globus SDK to lookup the service URL from
    # config. However, FuncX has its own logic for determining the base URL.
    # set `service_name` to allow the check which ensures this is set to pass
    # it does not have any other effects
    service_name: str = "funcx"
    # use the FuncX-specific error class
    error_class = GlobusAPIError

    def __init__(
        self,
        *,
        environment: t.Optional[str] = None,
        base_url: t.Optional[str] = None,
        app_name: t.Optional[str] = None,
        **kwargs,
    ):
        if base_url is None:
            base_url = get_web_service_url(environment)
        super().__init__(environment=environment, base_url=base_url, **kwargs)
        self._user_app_name = None
        self.user_app_name = app_name

    def get_version(self, *, service: str = "all") -> globus_sdk.GlobusHTTPResponse:
        return self.get("version", query_params={"service": service})

    def get_taskgroup_tasks(
        self, task_group_id: ID_PARAM_T
    ) -> globus_sdk.GlobusHTTPResponse:
        return self.get(f"/taskgroup/{task_group_id}")

    def get_task(self, task_id: ID_PARAM_T) -> globus_sdk.GlobusHTTPResponse:
        return self.get(f"tasks/{task_id}")

    def get_batch_status(
        self,
        task_ids: t.Iterable[ID_PARAM_T],
        *,
        additional_fields: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> globus_sdk.GlobusHTTPResponse:
        if isinstance(task_ids, str):
            task_ids = [task_ids]
        data = {"task_ids": [str(t) for t in task_ids]}
        if additional_fields is not None:
            data.update(additional_fields)
        return self.post("/batch_status", data=data)

    # the FuncXClient needs to send version information through BaseClient.app_name,
    # so that's overridden here to prevent direct manipulation. use user_app_name
    # instead to send any custom metadata through the User Agent request header
    @property
    def app_name(self) -> t.Optional[str]:
        return super().app_name

    @app_name.setter
    def app_name(self, value) -> None:
        raise NotImplementedError("Use user_app_name instead")

    # support custom user extensions of the default funcx app name
    @property
    def user_app_name(self):
        return self._user_app_name

    @user_app_name.setter
    def user_app_name(self, value):
        self._user_app_name = value
        app_name = user_agent_substring(__version__)
        if value is not None:
            app_name += f"/{value}"
        globus_sdk.BaseClient.app_name.fset(self, app_name)

    def submit(self, batch: t.Dict[str, t.Any]) -> globus_sdk.GlobusHTTPResponse:
        return self.post("submit", data=batch)

    def register_endpoint(
        self,
        endpoint_name: str,
        endpoint_id: ID_PARAM_T,
        *,
        metadata: t.Optional[dict] = None,
        multi_tenant: t.Optional[bool] = None,
        additional_fields: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> globus_sdk.GlobusHTTPResponse:
        data: t.Dict[str, t.Any] = {
            "endpoint_name": endpoint_name,
            "endpoint_uuid": str(endpoint_id),
        }

        # Only send this param if True.  Will have to change to
        # `if multi_tenant is not None` if we want to always pass it
        if multi_tenant:
            data["multi_tenant"] = multi_tenant

        if metadata:
            data["metadata"] = metadata
        if additional_fields is not None:
            data.update(additional_fields)
        return self.post("/endpoints", data=data)

    def get_result_amqp_url(self) -> globus_sdk.GlobusHTTPResponse:
        return self.get("get_amqp_result_connection_url")

    def get_endpoint_status(
        self, endpoint_id: ID_PARAM_T
    ) -> globus_sdk.GlobusHTTPResponse:
        return self.get(f"endpoints/{endpoint_id}/status")

    def get_endpoint_metadata(
        self, endpoint_id: ID_PARAM_T
    ) -> globus_sdk.GlobusHTTPResponse:
        return self.get(f"endpoints/{endpoint_id}")

    def get_endpoints(self) -> globus_sdk.GlobusHTTPResponse:
        return self.get("/endpoints")

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
        return self.post("/functions", data=data)

    def get_whitelist(self, endpoint_id: ID_PARAM_T) -> globus_sdk.GlobusHTTPResponse:
        return self.get(f"/endpoints/{endpoint_id}/whitelist")

    def whitelist_add(
        self, endpoint_id: ID_PARAM_T, function_ids: t.Iterable[ID_PARAM_T]
    ) -> globus_sdk.GlobusHTTPResponse:
        if isinstance(function_ids, str):
            function_ids = [function_ids]
        data = {"func": [str(f) for f in function_ids]}
        return self.post(f"/endpoints/{endpoint_id}/whitelist", data=data)

    def whitelist_remove(
        self, endpoint_id: ID_PARAM_T, function_id: ID_PARAM_T
    ) -> globus_sdk.GlobusHTTPResponse:
        return self.delete(f"/endpoints/{endpoint_id}/whitelist/{function_id}")

    def stop_endpoint(self, endpoint_id: ID_PARAM_T) -> globus_sdk.GlobusHTTPResponse:
        return self.post(f"/endpoints/{endpoint_id}/lock", data={})
