""" Globus Compute, formerly funcX: Fast function serving for clouds, clusters and supercomputers.
"""
from funcx.version import __version__ as _version

__author__ = "The Globus Compute team"
__version__ = _version

from globus_compute_sdk import Client as FuncXClient
from globus_compute_sdk import Executor as FuncXExecutor

__all__ = ("FuncXExecutor", "FuncXClient")
