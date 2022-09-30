from __future__ import annotations

import typing as t

from funcx.serialize import FuncXSerializer


class Batch:
    """Utility class for creating batch submission in funcX"""

    def __init__(self, task_group_id=None, create_websocket_queue=False):
        """
        Parameters
        ==========

        task_group_id : str
            UUID indicating the task group that this batch belongs to
        """
        self.tasks: list[dict[str, str]] = []
        self.fx_serializer = FuncXSerializer()
        self.task_group_id = task_group_id
        self.create_websocket_queue = create_websocket_queue

    def add(
        self,
        function_id: str,
        endpoint_id: str,
        args: tuple[t.Any, ...] | None = None,
        kwargs: dict[str, t.Any] | None = None,
    ) -> None:
        """Add a function invocation to a batch submission

        Parameters
        ----------
        function_id : uuid str
            Function UUID string. Required
        endpoint_id : uuid str
            Endpoint UUID string. Required
        args : tuple[Any, ...]
            Arguments as specified by the function signature
        kwargs : dict[str, Any]
            Keyword arguments as specified by the function signature

        Returns
        -------
        None
        """
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        ser_args = self.fx_serializer.serialize(args)
        ser_kwargs = self.fx_serializer.serialize(kwargs)
        payload = self.fx_serializer.pack_buffers([ser_args, ser_kwargs])

        data = {"endpoint": endpoint_id, "function": function_id, "payload": payload}

        self.tasks.append(data)

    def prepare(self):
        """Prepare the payloads to be post to web service in a batch

        Parameters
        ----------

        Returns
        -------
        payloads in dictionary, Dict[str, list]
        """
        data = {
            "task_group_id": self.task_group_id,
            "tasks": [],
            "create_websocket_queue": self.create_websocket_queue,
        }

        for task in self.tasks:
            new_task = (task["function"], task["endpoint"], task["payload"])
            data["tasks"].append(new_task)

        return data
