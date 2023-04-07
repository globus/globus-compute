from abc import ABCMeta, abstractmethod


class DeserializationError(Exception):
    """Base class for all deserialization errors"""

    def __init__(self, reason):
        self.reason = reason

    def __repr__(self):
        return f"Deserialization failed due to {self.reason}"

    def __str__(self):
        return self.__repr__()


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

    def check(self, payload):
        try:
            x = self.serialize(payload)
            self.deserialize(x)

        except Exception as e:
            raise SerializerError(f"Serialize-Deserialize combo failed due to {e}")

    @abstractmethod
    def serialize(self, data):
        raise NotImplementedError("Concrete class did not implement serialize")

    @abstractmethod
    def deserialize(self, payload):
        raise NotImplementedError("Concrete class did not implement deserialize")


class SerializerError:
    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return self.reason

    def __repr__(self):
        return self.__str__()
