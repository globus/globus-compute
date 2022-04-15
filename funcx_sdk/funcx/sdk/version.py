from __future__ import annotations

import typing as t

# single source of truth for package version,
# see https://packaging.python.org/en/latest/single_source_version/
__version__ = "0.4.0a1"


VersionType = t.Union[t.Tuple[int, int, int], t.Tuple[int, int, int, str, int]]


# parse to a tuple
def parse_version(s: str) -> VersionType:
    pre: tuple[()] | tuple[str, int] = ()
    vals = s.split(".")
    if len(vals) != 3:
        raise ValueError("bad version")
    major = int(vals[0])
    minor = int(vals[1])
    patch_s: str = vals[2]
    # handle pre versions:
    #   X.Y.ZaN
    #   X.Y.ZbN
    #   X.Y.Z-dev
    # no other forms supported (for now)
    if patch_s.endswith("-dev"):
        pre = ("dev", 0)
        patch = int(patch_s.rsplit("-", 1)[0])
    elif "a" in patch_s or "b" in patch_s:
        pre_tag = "a" if "a" in patch_s else "b"
        split_patch_s = patch_s.split(pre_tag)
        if len(split_patch_s) != 2:
            raise ValueError("bad version")
        patch, pre_val = int(split_patch_s[0]), int(split_patch_s[1])
        pre = (pre_tag, pre_val)
    else:
        patch = int(patch_s)

    return t.cast(VersionType, (major, minor, patch) + pre)


PARSED_VERSION = parse_version(__version__)


# app name to send as part of SDK requests
app_name = f"funcX SDK v{__version__}"
