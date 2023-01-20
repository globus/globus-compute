# single source of truth for package version,
# see https://packaging.python.org/en/latest/single_source_version/
__version__ = "1.0.8a0"

# TODO: remove after a `funcx` release
# this variable is needed because it's imported by `funcx` to do the version check
VERSION = __version__

# app name to send as part of requests
app_name = f"funcX Endpoint v{__version__}"
