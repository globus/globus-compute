from __future__ import annotations

import typing as t
from collections import defaultdict

from globus_compute_sdk.sdk.utils.uuid_like import UUID_LIKE_T
from globus_compute_sdk.serialize import ComputeSerializer

_default_serde = ComputeSerializer()


class Batch:
    """Utility class for creating batch submission in Globus Compute"""

    def __init__(
        self,
        task_group_id: UUID_LIKE_T | None,
        user_endpoint_config: dict[str, t.Any] | None = None,
        request_queue=False,
        serializer: ComputeSerializer | None = None,
    ):
        """
        :param task_group_id: UUID of task group to which to submit the batch
        :user_endpoint_config: User endpoint configuration values as described and
            allowed by endpoint administrators
        :param request_queue: Whether to request a result queue from the web service;
            typically only used by the Executor
        :param serialization_strategy: The strategy to use when serializing task
            arguments
        """
        self.task_group_id = task_group_id
        self.user_endpoint_config = user_endpoint_config
        self.tasks: dict[str, list[str]] = defaultdict(list)
        self._serde = serializer or _default_serde
        self.request_queue = request_queue

    def __repr__(self):
        return str(self.prepare())

    def __bool__(self):
        """Return true if all functions in batch have at least one task"""
        return all(bool(fns) for fns in self.tasks.values())

    def __len__(self):
        """Return the total number of tasks in batch (includes all functions)"""
        return sum(len(fns) for fns in self.tasks.values())

    def add(
        self,
        function_id: UUID_LIKE_T,
        args: tuple[t.Any, ...] | None = None,
        kwargs: dict[str, t.Any] | None = None,
    ) -> None:
        """
        Add a function invocation to a batch submission

        :param function_id : UUID of registered function as registered.  (Required)
        :param args: arguments as required by the function signature
        :param kwargs: Keyword arguments as required by the function signature
        """
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        ser_args = self._serde.serialize(args)
        ser_kwargs = self._serde.serialize(kwargs)
        payload = self._serde.pack_buffers([ser_args, ser_kwargs])

        self.tasks[str(function_id)].append(payload)

    def prepare(self) -> dict[str, str | list[tuple[str, str, str]]]:
        """
        Prepare the payload to be POSTed to web service in a batch

        :returns: a dictionary suitable for JSONification for POSTing to the web service
        """
        data = {
            "create_queue": self.request_queue,
            "tasks": dict(self.tasks),
        }
        if self.task_group_id:
            data["task_group_id"] = str(self.task_group_id)
        if self.user_endpoint_config:
            data["user_endpoint_config"] = self.user_endpoint_config

        return data
