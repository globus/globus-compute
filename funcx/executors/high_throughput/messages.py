import uuid
from abc import ABC, abstractmethod
from enum import Enum, auto
from struct import Struct

MESSAGE_TYPE_FORMATTER = Struct('b')


class MessageType(Enum):
    HEARTBEAT_REQ = auto()
    HEARTBEAT = auto()

    def pack(self):
        return MESSAGE_TYPE_FORMATTER.pack(self.value)

    @classmethod
    def unpack(cls, buffer):
        mtype, = MESSAGE_TYPE_FORMATTER.unpack_from(buffer, offset=0)
        return MessageType(mtype), buffer[MESSAGE_TYPE_FORMATTER.size:]


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
            return HeartbeatReq.unpack(msg)
        elif message_type is MessageType.HEARTBEAT:
            return Heartbeat.unpack(msg)

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
    _header_formatter = Struct('16s')  # header contains number of task statuses in payload
    _payload_formatter = Struct('16sb')  # need to think of appropriate structure?  Task id and 1 byte code?

    type = MessageType.HEARTBEAT

    def __init__(self, endpoint_id, statuses):
        super().__init__()
        endpoint_id_bytes = uuid.UUID(endpoint_id).bytes
        self._header = self._header_formatter.pack(endpoint_id_bytes)
        self._payload = statuses

    @classmethod
    def unpack(cls, msg):
        endpoint_id, = cls._header_formatter.unpack_from(msg)
        msg = msg[cls._header_formatter.size:]
        statuses = []
        for chonk in cls._payload_formatter.iter_unpack(msg):
            task_bytes, status_code = chonk
            task_id = str(uuid.UUID(bytes=task_bytes))
            statuses.append({
                "task_id": task_id,
                "status_code": status_code
            })
        return cls(endpoint_id, statuses)

    def pack(self):
        n = len(self._payload)
        buffer = bytes([0] * n * self._payload_formatter.size)
        for i, status in enumerate(self.payload):
            self._payload_formatter.pack_into(
                buffer, i * self._payload_formatter.size,
                status['task_id'], status['status_code']
            )
        return self.type.pack() + self._header + buffer

