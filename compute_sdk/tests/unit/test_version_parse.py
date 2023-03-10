import pytest
from globus_compute_sdk.errors import VersionMismatch
from globus_compute_sdk.version import compare_versions


@pytest.mark.parametrize(
    "current_v,min_v,expect_ok",
    [
        # non pre-versions
        ("1.2.3", "1.2.3", True),
        ("1.2.3", "1.2.4", False),
        # 'dev' allowed to match if the release portions are equal
        ("1.2.3-dev", "1.2.3", True),
        ("1.2.3a1", "1.2.3-dev", True),
        ("1.2.3-dev", "1.2.3b2", True),
        # dev versions take a back-seat to mistmatched MAJOR.MINOR.PATCH
        ("1.2.3", "1.2.4-dev", False),  # dev vs non-pre
    ],
)
def test_compare_versions(current_v, min_v, expect_ok):
    if expect_ok:
        compare_versions(current_v, min_v)
    else:
        with pytest.raises(VersionMismatch):
            compare_versions(current_v, min_v)
