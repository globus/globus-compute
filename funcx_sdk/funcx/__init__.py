""" funcX : Fast function serving for clouds, clusters and supercomputers.

"""
from funcx.version import __version__ as _version

__author__ = "The funcX team"
__version__ = _version

from funcx.sdk.client import FuncXClient
from funcx.sdk.errors import FuncxAPIError
from funcx.sdk.executor import FuncXExecutor

__all__ = ("FuncXExecutor", "FuncXClient", "FuncxAPIError")
