""" Globus Compute : Fast function serving for clouds, clusters and supercomputers.

"""

from globus_compute_sdk.version import __version__ as _version

__author__ = "The Globus Compute Team"
__version__ = _version

from globus_compute_sdk.sdk.client import Client
from globus_compute_sdk.sdk.container_spec import ContainerSpec
from globus_compute_sdk.sdk.executor import Executor
from globus_compute_sdk.sdk.mpi_function import MPIFunction
from globus_compute_sdk.sdk.shell_function import ShellFunction, ShellResult

__all__ = (
    "Client",
    "ContainerSpec",
    "Executor",
    "MPIFunction",
    "ShellFunction",
    "ShellResult",
)
