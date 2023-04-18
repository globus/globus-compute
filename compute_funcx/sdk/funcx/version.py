from globus_compute_sdk.errors import VersionMismatch
from packaging.version import Version

# single source of truth for package version,
# see https://packaging.python.org/en/latest/single_source_version/
__version__ = "2.0.1"

DEPRECATION_FUNCX = """
The funcX SDK has been renamed to Globus Compute SDK and the new package is
available on PyPI:
    https://pypi.org/project/globus-compute-sdk/

Please consider upgrading to Globus Compute.  More information can be found at:
    https://globus-compute.readthedocs.io/en/latest/funcx_upgrade.html
"""


def compare_versions(
    current: str, min_version: str, *, package_name: str = "funcx"
) -> None:
    current_v = Version(current)
    min_v = Version(min_version)

    if (
        current_v.is_devrelease
        or min_v.is_devrelease
        and current_v.release == min_v.release
    ):
        return

    if current_v < min_v:
        raise VersionMismatch(
            f"Your version={current} is lower than the "
            f"minimum version for {package_name}: {min_version}.  "
            "Please update. "
            f"pip install {package_name}>={min_version}"
        )
