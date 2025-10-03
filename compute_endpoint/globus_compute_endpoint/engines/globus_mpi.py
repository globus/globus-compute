import parsl.executors
from globus_compute_endpoint.engines.globus_compute import GlobusComputeEngine


class GlobusMPIEngine(GlobusComputeEngine):
    # There's no programmatic need for this interstitial __init__; it's here merely to
    # hold the documentation (for Sphinx and the build process).  A naive attempt
    # to place the documentation on the class also pulled in the parent class
    # documentation.

    _ExecutorClass = parsl.executors.MPIExecutor

    def __init__(self, *args, **kwargs):
        """``GlobusMPIEngine`` extends |GCE|_ and is a shim over Parsl's |MPIExecutor|_.
        For a complete list of available arguments, please reference the documentation
        for those classes.

        .. |GCE| replace:: ``GlobusComputeEngine``
        .. _GCE: engine.html
        .. |MPIExecutor| replace:: ``MPIExecutor``
        .. _MPIExecutor: https://parsl.readthedocs.io/en/stable/stubs/parsl.executors.MPIExecutor.html
        """  # noqa: E501

        super().__init__(*args, **kwargs)
