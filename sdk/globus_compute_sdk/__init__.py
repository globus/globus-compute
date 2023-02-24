""" funcX : Fast function serving for clouds, clusters and supercomputers.

"""
from sdk.version import __version__ as _version

__author__ = "The Globus Compute Team"
__version__ = _version

from sdk.sdk.client import Client
from sdk.sdk.container_spec import ContainerSpec
from sdk.sdk.executor import Executor

__all__ = ("Executor", "Client", "ContainerSpec")
