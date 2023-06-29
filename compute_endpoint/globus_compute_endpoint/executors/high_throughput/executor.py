import warnings

from globus_compute_endpoint.engines import HighThroughputEngine

warnings.warn(
    f"{__name__} is deprecated.  Please import from globus_compute_sdk.engines instead"
)


def HighThroughputExecutor(*args, **kwargs) -> HighThroughputEngine:
    warnings.warn(
        "HighThroughputExecutor is deprecated.  Please use HighThroughputEngine instead"
    )
    return HighThroughputEngine(*args, **kwargs)
