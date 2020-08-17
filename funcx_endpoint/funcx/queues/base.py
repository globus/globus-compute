from abc import ABCMeta, abstractmethod, abstractproperty
from funcx.utils.errors import FuncxError


class NotConnected(FuncxError):
    """ Queue is not connected/active
    """

    def __init__(self, queue):
        self.queue = queue

    def __repr__(self):
        return "Queue {} is not connected. Cannot execute queue operations".format(self.queue)


class FuncxQueue(metaclass=ABCMeta):
    """ Queue interface required by the Forwarder

    This is a metaclass that only enforces concrete implementations of
    functionality by the child classes.
    """

    @abstractmethod
    def connect(self, *args, **kwargs):
        """ Connects and creates the queue.
        The queue is not active until this is called
        """
        pass

    @abstractmethod
    def get(self, *args, **kwargs):
        """ Get an item from the Queue
        """
        pass

    @abstractmethod
    def put(self, *args, **kwargs):
        """ Put an item into the Queue
        """
        pass

    @abstractproperty
    def is_connected(self):
        """ Returns the connected status of the queue.

        Returns
        -------
             Bool
        """
        pass
