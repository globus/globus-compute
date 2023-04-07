""" Globus Compute, formerly funcX: Fast function serving for clouds,
 clusters and supercomputers.
"""
import warnings

from funcx.version import DEPRECATION_FUNCX
from funcx.version import __version__ as _version

__author__ = "The Globus Compute team"
__version__ = _version

from globus_compute_sdk import Client as FuncXClient
from globus_compute_sdk import Executor as FuncXExecutor

warnings.warn(DEPRECATION_FUNCX)

__all__ = ("FuncXExecutor", "FuncXClient")
