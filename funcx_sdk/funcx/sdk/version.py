from __future__ import annotations

import typing as t

# single source of truth for package version,
# see https://packaging.python.org/en/latest/single_source_version/
__version__ = "0.3.10-dev"


VersionType = t.Union[t.Tuple[int, int, int], t.Tuple[int, int, int, str]]


# parse to a tuple
def parse_version(s: str) -> VersionType:
    pre: tuple[str, ...] = ()
    if s.endswith("-dev"):
        pre = ("dev", 0)
        s = s.rsplit("-", 1)[0]
    vals = s.split(".")
    if len(vals) != 3:
        raise ValueError("bad version")
    return t.cast(VersionType, tuple(int(x) for x in vals) + pre)


PARSED_VERSION = parse_version(__version__)


# app name to send as part of SDK requests
app_name = f"funcX SDK v{__version__}"
