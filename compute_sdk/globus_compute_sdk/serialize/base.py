from abc import ABCMeta, abstractmethod


class SerializeBase(metaclass=ABCMeta):
    """Shared functionality for all serializer implementations"""

    @property
    @abstractmethod
    def identifier(self):
        pass

    def chomp(self, payload: str) -> str:
        """If the payload starts with the identifier, return the remaining block

        Parameters
        ----------
        payload : str
            Payload blob
        """
        s_id, payload = payload.split("\n", 1)
        if (s_id + "\n") != self.identifier:
            raise DeserializationError(
                f"Buffer does not start with identifier:{self.identifier}"
            )
        return payload

    @abstractmethod
    def serialize(self, data):
        raise NotImplementedError("Concrete class did not implement serialize")

    @abstractmethod
    def deserialize(self, payload):
        raise NotImplementedError("Concrete class did not implement deserialize")


class SerializerError:
    def __init__(self, reason):
        self.reason = reason

    def __repr__(self):
        return f"Serialization failed due to {self.reason}"


class DeserializationError(Exception):
    """Base class for all deserialization errors"""

    def __init__(self, reason):
        self.reason = reason

    def __repr__(self):
        return f"Deserialization failed due to {self.reason}"
