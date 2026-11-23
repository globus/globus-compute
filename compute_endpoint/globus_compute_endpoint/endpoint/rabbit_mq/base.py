"""
common elements to rabbitmq tools
"""

import enum

import pika


class RabbitPublisherStatus(enum.Enum):
    closed = enum.auto()
    connected = enum.auto()


# a status enum to describe the different states of the subscriber subprocs
class SubscriberProcessStatus(enum.Enum):
    # the object is in the parent process, it has no notion of the process state other
    # than that it is none of the other values
    parent = enum.auto()
    # the process is in the child proc and should try to start or continue to run
    running = enum.auto()
    # the process is in the child proc and is closing down, it should not try to
    # reconnect or otherwise handle failures
    closing = enum.auto()


# Utility method to verify type to avoid mypy typing error
def verify_is_base_connection(conn: pika.connection.Connection) -> pika.BaseConnection:
    if not isinstance(conn, pika.BaseConnection):
        raise TypeError(f"Expected a BaseConnection, got {type(conn).__name__}")
    return conn
