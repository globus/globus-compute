import uuid
from abc import ABC, abstractmethod
from enum import Enum
from struct import Struct


MESSAGE_TYPE_FORMATTER = Struct('b')


class MessageType(Enum):
    STOP = 0
    HEARTBEAT_REQ = 1
    HEARTBEAT = 2
    STATUS_REQUEST = 3
    TASK = 4

    def pack(self):
        return MESSAGE_TYPE_FORMATTER.pack(self.value)

    @classmethod
    def unpack(cls, buffer):
        val = MESSAGE_TYPE_FORMATTER.unpack_from(buffer, offset=0)
        return MessageType(val), buffer[MESSAGE_TYPE_FORMATTER.size:]


class Message(ABC):
    def __init__(self):
        self._payload = None

    @property
    def type(self):
        raise NotImplementedError()

    @property
    def header(self):
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
        pass

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

    _header_formatter = Struct('I')  # header contains number of task statuses in payload
    _payload_formatter = Struct('16sb')  # need to think of appropriate structure?  Task id and 1 byte code?

    type = MessageType.HEARTBEAT

    def __init__(self, statuses):
        super().__init__()
        self._payload = statuses

    @property
    def header(self):
        return len(self._payload)

    @classmethod
    def unpack(cls, msg):
        header = cls._header_formatter.unpack_from(msg)
        msg = msg[cls._header_formatter.size:]
        statuses = []
        for chonk in cls._payload_formatter.iter_unpack(msg):
            task_bytes, status_code = chonk
            task_id = str(uuid.UUID(bytes=task_bytes))
            statuses.append({
                "task_id": task_id,
                "status_code": status_code
            })
        return

    def pack(self):
        n = len(self.payload)
        header = self._header_formatter.pack()
        buffer = bytes([0]*n*self._payload_formatter.size)
        for i, status in enumerate(self.payload):
            self._payload_formatter.pack_into(
                buffer, i*self._payload_formatter.size,
                status['task_id'], status['status_code']
            )
        return self.type.pack() + header + buffer

class TaskMessage:
    def __init__(self):
        pass

    @classmethod
    def parse(cls, msg):
        return cls()


class TaskHeader:
    """Struct for packing/unpacking headers that store routing information about tasks.

        """
    RAW_CONTAINER_BYTES = b'RAW\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'
    RAW_CONTAINER = 'RAW'
    formatter = Struct('16s16sb')

    def __init__(self, task_id, container_id, retries):
        self.task_id = task_id
        self.container_id = container_id
        self.retries = retries

    def pack(self):
        if self.container_id != TaskHeader.RAW_CONTAINER:
            container_id_bytes = uuid.UUID(self.container_id).bytes
        else:
            container_id_bytes = TaskHeader.RAW_CONTAINER_BYTES
        task_id_bytes = uuid.UUID(self.task_id).bytes
        return self.formatter.pack(task_id_bytes, container_id_bytes, self.retries)

    @classmethod
    def unpack(cls, buffer):
        """
        Unpack the header data,
        Parameters
        ----------
        buffer

        Returns
        -------

        """
        task, container, retries = cls.formatter.unpack_from(buffer, offset=0)
        task = str(uuid.UUID(bytes=task))
        if container == TaskHeader.RAW_CONTAINER_BYTES:
            container = TaskHeader.RAW_CONTAINER
        else:
            container = str(uuid.UUID(bytes=container))
        return cls(task, container, retries), buffer[cls.formatter.size:]


class OutstandingCountReq(Message):
    pass


class OutstandingCount(Message):
    pass


class ListManagers(Message):
    pass


class ManagersList(Message):
    pass


class HoldWorker(Message):
    def __init__(self, worker):
        self._payload = worker