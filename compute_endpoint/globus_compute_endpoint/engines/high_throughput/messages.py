from __future__ import annotations

import json
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from enum import Enum, auto
from struct import Struct

from globus_compute_common.messagepack.message_types import TaskTransition

MESSAGE_TYPE_FORMATTER = Struct("b")


class MessageType(Enum):
    HEARTBEAT_REQ = auto()
    HEARTBEAT = auto()
    EP_STATUS_REPORT = auto()
    MANAGER_STATUS_REPORT = auto()
    TASK = auto()
    RESULTS_ACK = auto()
    TASK_CANCEL = auto()
    BAD_COMMAND = auto()

    def pack(self):
        return MESSAGE_TYPE_FORMATTER.pack(self.value)

    @classmethod
    def unpack(cls, buffer):
        (mtype,) = MESSAGE_TYPE_FORMATTER.unpack_from(buffer, offset=0)
        return MessageType(mtype), buffer[MESSAGE_TYPE_FORMATTER.size :]


COMMAND_TYPES = {MessageType.HEARTBEAT_REQ, MessageType.TASK_CANCEL}


class Message(ABC):
    def __init__(self):
        self._payload = None
        self._header = None

    @property
    def header(self):
        return self._header

    @property
    def type(self):
        raise NotImplementedError()

    @property
    def payload(self):
        return self._payload

    @classmethod
    def unpack(cls, msg):
        message_type, remaining = MessageType.unpack(msg)
        if message_type is MessageType.HEARTBEAT_REQ:
            return HeartbeatReq.unpack(remaining)
        elif message_type is MessageType.HEARTBEAT:
            return Heartbeat.unpack(remaining)
        elif message_type is MessageType.EP_STATUS_REPORT:
            return EPStatusReport.unpack(remaining)
        elif message_type is MessageType.MANAGER_STATUS_REPORT:
            return ManagerStatusReport.unpack(remaining)
        elif message_type is MessageType.TASK:
            return Task.unpack(remaining)
        elif message_type is MessageType.RESULTS_ACK:
            return ResultsAck.unpack(remaining)
        elif message_type is MessageType.TASK_CANCEL:
            return TaskCancel.unpack(remaining)
        elif message_type is MessageType.BAD_COMMAND:
            return BadCommand.unpack(remaining)

        raise Exception(f"Unknown Message Type Code: {message_type}")

    @abstractmethod
    def pack(self):
        raise NotImplementedError()


class Task(Message):
    """
    Task message from the forwarder->interchange
    """

    type = MessageType.TASK

    def __init__(
        self, task_id: str, container_id: str, task_buffer: str | bytes, raw_buffer=None
    ):
        super().__init__()
        self.task_id = task_id
        self.container_id = container_id
        self.task_buffer = task_buffer
        self.raw_buffer = raw_buffer

    def pack(self) -> bytes:
        if self.raw_buffer is None:
            # a type:ignore is needed here
            # task_buffer might be a str  or it might be a bytes (see `unpack`)
            # and we can't tell which is correct
            #
            # rather than thinking hard, preserve the exact current runtime behavior
            #
            # all of this code is going to be eliminated soonish by
            # globus_compute_common.messagepack in part because of issues like this
            add_ons = (
                f"TID={self.task_id};CID={self.container_id};"  # type: ignore
                f"{self.task_buffer}"
            )
            self.raw_buffer = add_ons.encode("utf-8")

        return self.type.pack() + self.raw_buffer

    @classmethod
    def unpack(cls, raw_buffer: bytes):
        b_tid, b_cid, task_buf = raw_buffer.decode("utf-8").split(";", 2)
        return cls(
            b_tid[4:], b_cid[4:], task_buf.encode("utf-8"), raw_buffer=raw_buffer
        )

    def set_local_container(self, container_id):
        self.local_container = container_id


class HeartbeatReq(Message):
    """
    Synchronous request for a Heartbeat.

    This is sent from the Forwarder to the endpoint on start to get an initial
    connection and ensure liveness.
    """

    type = MessageType.HEARTBEAT_REQ

    @property
    def header(self):
        return None

    @property
    def payload(self):
        return None

    @classmethod
    def unpack(cls, msg):
        return cls()

    def pack(self):
        return self.type.pack()


class Heartbeat(Message):
    """
    Generic Heartbeat message, sent in both directions between Forwarder and
    Interchange.
    """

    type = MessageType.HEARTBEAT

    def __init__(self, endpoint_id):
        super().__init__()
        self.endpoint_id = endpoint_id

    @classmethod
    def unpack(cls, msg):
        return cls(msg.decode("ascii"))

    def pack(self):
        return self.type.pack() + self.endpoint_id.encode("ascii")


class EPStatusReport(Message):
    """
    Status report for an endpoint, sent from Interchange to Forwarder.

    Includes EP-wide info such as utilization, as well as per-task status information.
    """

    type = MessageType.EP_STATUS_REPORT

    def __init__(self, endpoint_id, global_state, task_statuses):
        super().__init__()
        self._header = uuid.UUID(endpoint_id).bytes
        self.global_state = global_state
        self.task_statuses = task_statuses

    def __repr__(self):
        name = type(self).__name__
        ep_id = uuid.UUID(bytes=self._header)
        return f"{name}(ep:{ep_id}; task count:{len(self.task_statuses)})"

    @classmethod
    def unpack(cls, msg):
        endpoint_id = str(uuid.UUID(bytes=msg[:16]))
        msg = msg[16:]
        jsonified = msg.decode("ascii")
        global_state, statuses = json.loads(jsonified)
        task_statuses = defaultdict(list)
        for tid, tt in statuses.items():
            for trans in tt:
                task_statuses[tid].append(
                    TaskTransition(
                        timestamp=trans["timestamp"],
                        actor=trans["actor"],
                        state=trans["state"],
                    )
                )
        return cls(endpoint_id, global_state, task_statuses)

    def pack(self):
        statuses = {}
        for tid, tt in self.task_statuses.items():
            for status in tt:
                statuses[tid] = statuses.get(tid, [])
                statuses[tid].append(status.to_dict())
        jsonified = json.dumps([self.global_state, statuses])
        return self.type.pack() + self._header + jsonified.encode("ascii")


class ManagerStatusReport(Message):
    """
    Status report sent from the Manager to the Interchange, which mostly just amounts
    to saying which tasks are now RUNNING.
    """

    type = MessageType.MANAGER_STATUS_REPORT

    def __init__(self, task_statuses, container_switch_count):
        super().__init__()
        self.task_statuses = task_statuses
        self.container_switch_count = container_switch_count

    @classmethod
    def unpack(cls, msg):
        container_switch_count = int.from_bytes(msg[:10], "little")
        msg = msg[10:]
        jsonified = msg.decode("ascii")
        statuses = json.loads(jsonified)
        task_statuses = defaultdict(list)
        for tid, tt in statuses.items():
            for trans in tt:
                task_statuses[tid].append(
                    TaskTransition(
                        timestamp=trans["timestamp"],
                        actor=trans["actor"],
                        state=trans["state"],
                    )
                )
        return cls(task_statuses, container_switch_count)

    def pack(self):
        # TODO: do better than JSON?
        statuses = {}
        for tid, tt in self.task_statuses.items():
            for status in tt:
                statuses[tid] = statuses.get(tid, [])
                statuses[tid].append(status.to_dict())

        jsonified = json.dumps(statuses)
        return (
            self.type.pack()
            + self.container_switch_count.to_bytes(10, "little")
            + jsonified.encode("ascii")
        )


class ResultsAck(Message):
    """
    Results acknowledgement to acknowledge a task result was received by
    the forwarder. Sent from forwarder->interchange
    """

    type = MessageType.RESULTS_ACK

    def __init__(self, task_id):
        super().__init__()
        self.task_id = task_id

    @classmethod
    def unpack(cls, msg):
        return cls(msg.decode("ascii"))

    def pack(self):
        return self.type.pack() + self.task_id.encode("ascii")


class TaskCancel(Message):
    """
    Synchronous request for to cancel a Task.

    This is sent from the Executor to the Interchange
    """

    type = MessageType.TASK_CANCEL

    def __init__(self, task_id):
        super().__init__()
        self.task_id = task_id

    @classmethod
    def unpack(cls, msg):
        return cls(json.loads(msg.decode("ascii")))

    def pack(self):
        return self.type.pack() + json.dumps(self.task_id).encode("ascii")


class BadCommand(Message):
    """
    Error message send to indicate that a command is either
    unknown, malformed or unsupported.
    """

    type = MessageType.BAD_COMMAND

    def __init__(self, reason: str):
        super().__init__()
        self.reason = reason

    @classmethod
    def unpack(cls, msg):
        return cls(msg.decode("ascii"))

    def pack(self):
        return self.type.pack() + self.reason.encode("ascii")
