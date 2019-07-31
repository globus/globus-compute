from abc import ABCMeta, abstractmethod, abstractproperty
from tblib import Traceback
import logging

# GLOBALS
METHODS_MAP_CODE = {}
METHODS_MAP_DATA = {}


class fxPicker_enforcer(metaclass=ABCMeta):
    """ Ensure that any concrete class will have the serialize and deserialize methods
    """

    @abstractmethod
    def serialize(self, data):
        pass

    @abstractmethod
    def deserialize(self, payload):
        pass


class fxPicker_shared(object):
    """ Adds shared functionality for all serializer implementations
    """

    def __init_subclass__(cls, *args, **kwargs):
        """ This forces all child classes to register themselves as
        methods for serializing code or data
        """
        super().__init_subclass__(*args, **kwargs)
        if cls._for_code :
            METHODS_MAP_CODE[cls._identifier] = cls
        else:
            METHODS_MAP_DATA[cls._identifier] = cls

    @property
    def identifier(self):
        """ Get the identifier of the serialization method

        Returns
        -------
        identifier : str
        """
        return self._identifier

    def chomp(self, payload):
        """ If the payload starts with the identifier, return the remaining block

        Parameters
        ----------
        payload : str
            Payload blob
        """
        if payload.startswith(self.identifier):
            return payload[len(self.identifier):]

    def check(self, payload):

        try:
            x = self.serialize(payload)
            self.deserialize(x)

        except Exception as e:
            raise SerializerError("Serialize-Deserialize combo failed due to {}".format(e))


class SerializerError:
    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return self.reason

    def __repr__(self):
        return self.__str__()

class RemoteExceptionWrapper:
    def __init__(self, e_type, e_value, traceback):

        self.e_type = e_type
        self.e_value = e_value
        self.e_traceback = traceback

    def reraise(self):
        logger.debug("Reraising exception of type {}".format(self.e_type))
        reraise(self.e_type, self.e_value, self.e_traceback)


