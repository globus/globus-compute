import pytest

from funcx.sdk.version import parse_version


@pytest.mark.parametrize(
    "as_str, as_tuple",
    [
        (("1.2.3"), (1, 2, 3)),
        (("0.3.9"), (0, 3, 9)),
        (("1.2.3-dev"), (1, 2, 3, "dev", 0)),
        (("0.3.9-dev"), (0, 3, 9, "dev", 0)),
    ],
)
def test_parse_version_ok(as_str, as_tuple):
    assert parse_version(as_str) == as_tuple


@pytest.mark.parametrize("bad_version", ["1.2", "1.2.3.4", "1.a.3", "1.0.0-dev-dev"])
def test_parse_version_bad_version(bad_version):
    with pytest.raises(ValueError):
        parse_version(bad_version)
