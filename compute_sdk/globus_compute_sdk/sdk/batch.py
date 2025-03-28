from __future__ import annotations

import typing as t
import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass

from globus_compute_sdk.sdk.utils.uuid_like import (
    UUID_LIKE_T,
    as_optional_uuid,
    as_uuid,
)
from globus_compute_sdk.serialize import ComputeSerializer

_default_serde = ComputeSerializer()


@dataclass
class UserRuntime:
    """Information about a user's runtime environment, which is sent along with task
    submissions to the MEP user config renderer.

    :param str globus_compute_sdk_version: Version of the Compute SDK
    :param str globus_sdk_version: Version of the Globus SDK
    :param str python_version: Python version running the Compute SDK
    """

    globus_compute_sdk_version: str
    globus_sdk_version: str
    python_version: str


class Batch:
    """Utility class for creating batch submission in Globus Compute"""

    def __init__(
        self,
        task_group_id: UUID_LIKE_T | None,
        resource_specification: dict[str, t.Any] | None = None,
        user_endpoint_config: dict[str, t.Any] | None = None,
        request_queue=False,
        serializer: ComputeSerializer | None = None,
        user_runtime: UserRuntime | None = None,
        result_serializers: list[str] | None = None,
    ):
        """
        :param task_group_id: UUID of task group to which to submit the batch
        :param resource_specification: Specify resource requirements for individual task
            execution
        :param user_endpoint_config: User endpoint configuration values as described and
            allowed by endpoint administrators
        :param request_queue: Whether to request a result queue from the web service;
            typically only used by the Executor
        :param serializer: Used to serialize task args and kwargs
        :param user_runtime: Information about the runtime used to create and prepare
            this batch, such as Python and Globus Compute SDK versions
        :param result_serializers: A list of serialization strategy import paths to
            pass to the endpoint for use in serializing execution results.
        """
        self.task_group_id = as_optional_uuid(task_group_id)
        self.resource_specification = resource_specification
        self.user_endpoint_config = user_endpoint_config
        self.tasks: dict[uuid.UUID, list[str]] = defaultdict(list)
        self._serde = serializer or _default_serde
        self.request_queue = request_queue
        self.user_runtime = user_runtime
        self.result_serializers = result_serializers

    def __repr__(self):
        return str(self.prepare())

    def __bool__(self):
        """Return true if all functions in batch have at least one task"""
        return all(bool(fns) for fns in self.tasks.values())

    def __len__(self):
        """Return the total number of tasks in batch (includes all functions)"""
        return sum(len(fns) for fns in self.tasks.values())

    @property
    def task_group_id(self) -> uuid.UUID | None:
        return self._task_group_id

    @task_group_id.setter
    def task_group_id(self, new_id: UUID_LIKE_T):
        self._task_group_id = as_optional_uuid(new_id)

    def add(
        self,
        function_id: UUID_LIKE_T,
        args: tuple[t.Any, ...] | None = None,
        kwargs: dict[str, t.Any] | None = None,
    ) -> None:
        """
        Add a function invocation to a batch submission

        :param function_id: UUID of registered function.  (Required)
        :param args: arguments as required by the function signature
        :param kwargs: Keyword arguments as required by the function signature
        """
        if args is None:
            args = ()
        if not isinstance(args, (list, tuple)):
            raise TypeError(f"args: expected `tuple`, got: {type(args).__name__}")

        if kwargs is None:
            kwargs = {}
        if not isinstance(kwargs, dict):
            k_type = type(kwargs).__name__  # type: ignore[unreachable]
            raise TypeError(f"kwargs: expected dict, got: {k_type}")
        elif not all(isinstance(k, str) for k in kwargs):
            k = next(_k for _k in kwargs if not isinstance(_k, str))
            k_type = type(k).__name__
            raise TypeError(f"kwargs: expected kwargs with `str` keys, got: {k_type}")

        ser_args = self._serde.serialize(args)
        ser_kwargs = self._serde.serialize(kwargs)
        payload = self._serde.pack_buffers([ser_args, ser_kwargs])

        # as_uuid to check type (per user typo identified in #help)
        self.tasks[as_uuid(function_id)].append(payload)

    def prepare(self) -> dict[str, str | list[tuple[str, str, str]]]:
        """
        Prepare the payload to be POSTed to web service in a batch

        :returns: a dictionary suitable for JSONification for POSTing to the web service
        """
        data = {
            "create_queue": self.request_queue,
            "tasks": {str(fn_id): tasks for fn_id, tasks in self.tasks.items()},
        }
        if self.task_group_id:
            data["task_group_id"] = str(self.task_group_id)
        if self.resource_specification:
            data["resource_specification"] = self.resource_specification
        if self.user_endpoint_config:
            data["user_endpoint_config"] = self.user_endpoint_config
        if self.user_runtime:
            data["user_runtime"] = asdict(self.user_runtime)
        if self.result_serializers:
            data["result_serializers"] = self.result_serializers

        return data
