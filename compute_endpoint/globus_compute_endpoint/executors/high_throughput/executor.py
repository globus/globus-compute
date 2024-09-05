import warnings

from globus_compute_endpoint.engines import HighThroughputEngine

warnings.warn(
    f"{__name__} is deprecated. Please import from globus_compute_sdk.engines instead.",
    category=DeprecationWarning,
    stacklevel=2,
)


def HighThroughputExecutor(*args, **kwargs) -> HighThroughputEngine:
    warnings.warn(
        "HighThroughputExecutor is deprecated. Please use GlobusComputeEngine instead.",
        category=DeprecationWarning,
        stacklevel=2,
    )
    return HighThroughputEngine(*args, **kwargs)
