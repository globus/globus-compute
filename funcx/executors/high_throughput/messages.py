import json
import uuid
from abc import ABC, abstractmethod
from enum import Enum, auto
from struct import Struct

MESSAGE_TYPE_FORMATTER = Struct('b')


class MessageType(Enum):
    HEARTBEAT_REQ = auto()
    HEARTBEAT = auto()
    EP_STATUS_REPORT = auto()

    def pack(self):
        return MESSAGE_TYPE_FORMATTER.pack(self.value)

    @classmethod
    def unpack(cls, buffer):
        mtype, = MESSAGE_TYPE_FORMATTER.unpack_from(buffer, offset=0)
        return MessageType(mtype), buffer[MESSAGE_TYPE_FORMATTER.size:]


class TaskStatusCode(int, Enum):
    WAITING_FOR_NODES = auto()
    WAITING_FOR_LAUNCH = auto()
    RUNNING = auto()
    SUCCESS = auto()
    FAILED = auto()


COMMAND_TYPES = {
    MessageType.HEARTBEAT_REQ
}


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

    @payload.setter
    def payload(self, p):
        return

    @classmethod
    def unpack(cls, msg):
        message_type, remaining = MessageType.unpack(msg)
        if message_type is MessageType.HEARTBEAT_REQ:
            return HeartbeatReq.unpack(remaining)
        elif message_type is MessageType.HEARTBEAT:
            return Heartbeat.unpack(remaining)
        elif message_type is MessageType.EP_STATUS_REPORT:
            return EPStatusReport.unpack(remaining)

        raise Exception(f"Unknown Message Type Code: {message_type}")

    @abstractmethod
    def pack(self):
        raise NotImplementedError()


class HeartbeatReq(Message):
    _header_formatter = Struct('')
    _payload_formatter = Struct('')
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
    # _payload_formatter = Struct('16sb')  # need to think of appropriate structure?  Task id and 1 byte code?

    type = MessageType.EP_STATUS_REPORT

    def __init__(self, endpoint_id, ep_status_report, task_statuses):
        super().__init__()
        self._header = uuid.UUID(endpoint_id).bytes
        self.ep_status = ep_status_report
        self.task_statuses = task_statuses
        # self._payload = task_statuses

    @classmethod
    def unpack(cls, msg):
        endpoint_id = str(uuid.UUID(bytes=msg[:16]))
        msg = msg[16:]
        jsonified = msg.decode("ascii")
        ep_status, task_statuses = json.loads(jsonified)
        return cls(endpoint_id, ep_status, task_statuses)

    def pack(self):
        # n = len(self._payload)
        # buffer = bytes([0] * n * self._payload_formatter.size)
        # for i, status in enumerate(self.payload):
        #     self._payload_formatter.pack_into(
        #         buffer, i * self._payload_formatter.size,
        #         status['task_id'], status['status_code']
        #     )
        jsonified = json.dumps([self.ep_status, self.task_statuses])
        return self.type.pack() + self._header + jsonified.encode("ascii")

