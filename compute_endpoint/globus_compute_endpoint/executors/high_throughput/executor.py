import logging

from globus_compute_endpoint.engines import HighThroughputEngine

log = logging.getLogger(__name__)

warning_message = (
    "WARNING: \n"
    "The HighThroughputExecutor will be deprecated by globus-compute-endpoint v2.2 "
    "and will be replaced by the HighThroughputEngine"
)


class HighThroughputExecutor:
    """The HighThroughputExecutor class is a wrapper
    over the engines.HighThroughputEngine
    """

    def __new__(cls, *args, **kwargs):
        print(warning_message)
        return object.__new__(HighThroughputEngine, *args, **kwargs)
