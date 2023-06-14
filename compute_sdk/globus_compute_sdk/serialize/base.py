from abc import ABCMeta, abstractmethod


class SerializationStrategy(metaclass=ABCMeta):
    """A SerializationStrategy is in charge of converting function source code or
    arguments into string data and back again.
    """

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


class SerializationError(Exception):
    def __init__(self, reason):
        self.reason = reason

    def __repr__(self):
        return f"Serialization failed due to {self.reason}"


class DeserializationError(Exception):
    def __init__(self, reason):
        self.reason = reason

    def __repr__(self):
        return f"Deserialization failed due to {self.reason}"
