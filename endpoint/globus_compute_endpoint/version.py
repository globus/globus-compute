# single source of truth for package version,
# see https://packaging.python.org/en/latest/single_source_version/
__version__ = "1.0.11"

# TODO: remove after a SDK release
# this variable is needed because it's imported by the SDK to do the version check
VERSION = __version__

# app name to send as part of requests
app_name = f"Globus Compute Endpoint v{__version__}"
