import uuid
from enum import Enum
from struct import Struct


MESSAGE_TYPE_FORMATTER = Struct('b')


class MessageType(Enum):
    STOP = 0
    HEARTBEAT = 1
    STATUS_REQUEST = 2
    TASK = 3

    def pack(self):
        return MESSAGE_TYPE_FORMATTER.pack(self.value)

    @classmethod
    def unpack(cls, buffer):
        val = MESSAGE_TYPE_FORMATTER.unpack_from(buffer, offset=0)
        return MessageType(val), buffer[MESSAGE_TYPE_FORMATTER.size:]


class TaskHeader:
    """Struct for packing/unpacking headers that store routing information about tasks.

        """
    formatter = Struct('16s16sb')

    def __init__(self, task_id, container_id, retries):
        self.task_id = task_id
        self.container_id = container_id
        self.retries = retries

    def pack(self):
        container_id_bytes = uuid.UUID(self.container_id).bytes
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
        container = str(uuid.UUID(bytes=container))
        return cls(task, container, retries), buffer[cls.formatter.size:]
