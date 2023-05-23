# single source of truth for package version,
# see https://packaging.python.org/en/latest/single_source_version/
__version__ = "2.1.0"

# TODO: remove after a `globus-compute-sdk` release
# this is needed because it's imported by `globus-compute-sdk` to do the version check
VERSION = __version__

# Here as it's the easier way for funcx-endpoint cli to display it
DEPRECATION_FUNCX_ENDPOINT = """
funcX Endpoint has been renamed to Globus Compute Endpoint and the new package
is available on PyPI:
    https://pypi.org/project/globus-compute-endpoint/

Please consider upgrading to Globus Compute.  More information can be found at:
    https://globus-compute.readthedocs.io/en/latest/funcx_upgrade.html
"""


# app name to send as part of requests
app_name = f"Globus Compute Endpoint v{__version__}"
