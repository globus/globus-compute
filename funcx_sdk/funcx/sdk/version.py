from __future__ import annotations

import typing as t

# single source of truth for package version,
# see https://packaging.python.org/en/latest/single_source_version/
__version__ = "0.4.0a1"


_StandardVersionType = t.Tuple[int, int, int]
_PreVersionType = t.Tuple[int, int, int, str, int]
VersionType = t.Union[_StandardVersionType, _PreVersionType]


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


def _simple_compare_versions(a: VersionType, b: VersionType) -> int:
    if a < b:
        return -1
    elif a == b:
        return 0
    else:
        return 1


def _compare_parsed_versions(a: VersionType, b: VersionType) -> int:
    """
    returns
      a < b: -1
      a == b: 0
      a > b: 1
    """
    if len(a) == len(b):
        # if versions are triples (no pre-version), simple comparison
        if len(a) == 3:
            return _simple_compare_versions(a, b)

        # length is equal, but these are pre-verisons and therefore expected to have a
        # length of 5
        # mypy won't infer this, so cast to the correct type
        a = t.cast(_PreVersionType, a)
        b = t.cast(_PreVersionType, b)

        # if the pre-version is equal (alpha-to-alpha, beta-to-beta, dev-to-dev)
        # do simple comparison
        if a[3] == b[3]:
            return _simple_compare_versions(a, b)

        # when MAJOR.MINOR.PATCH is equivalent, compare by specifiers
        if a[:3] == b[:3]:
            # dev versions "equal everything"
            if a[3] == "dev" or b[3] == "dev":
                return 0
            # alpha < beta
            if a[3] == "a" and b[3] == "b":
                return -1
            elif a[3] == "b" and b[3] == "a":
                return 1

    # if lengths are not equal, only one of these is a pre-version
    # or perhaps they are both pre-versions, but with different MAJOR.MINOR.PATCH
    # trim to triples and compare
    return _simple_compare_versions(a[:3], b[:3])


def compare_versions(a: VersionType | str, b: VersionType | str) -> int:
    if isinstance(a, str):
        a = parse_version(a)
    if isinstance(b, str):
        b = parse_version(b)
    return _compare_parsed_versions(a, b)


PARSED_VERSION = parse_version(__version__)

# app name to send as part of SDK requests
app_name = f"funcX SDK v{__version__}"
