import pytest

from funcx.sdk.version import compare_versions, parse_version


@pytest.mark.parametrize(
    "as_str, as_tuple",
    [
        (("1.2.3"), (1, 2, 3)),
        (("0.3.9"), (0, 3, 9)),
        (("1.2.3-dev"), (1, 2, 3, "dev", 0)),
        (("0.3.9-dev"), (0, 3, 9, "dev", 0)),
        (("0.4.0a1"), (0, 4, 0, "a", 1)),
    ],
)
def test_parse_version_ok(as_str, as_tuple):
    assert parse_version(as_str) == as_tuple


@pytest.mark.parametrize(
    "bad_version",
    ["1.2", "1.2.3.4", "1.a.3", "1.0.0-dev-dev", "0.1.1aa2", "0.1.1a-dev"],
)
def test_parse_version_bad_version(bad_version):
    with pytest.raises(ValueError):
        parse_version(bad_version)


@pytest.mark.parametrize(
    "left,right,expect_result",
    [
        # non pre-versions
        ("1.2.3", "1.2.3", 0),
        ("1.2.3", "1.2.4", -1),
        # 'dev' allowed to match anything if the non-pre versions are equal
        ("1.2.3-dev", "1.2.3", 0),
        ("1.2.3-dev", "1.2.3-dev", 0),
        ("1.2.3a1", "1.2.3-dev", 0),
        ("1.2.3-dev", "1.2.3b2", 0),
        # alpha and beta comparisons work as expected
        ("1.2.3a1", "1.2.3a1", 0),  # equal alphas
        ("1.2.3a2", "1.2.3a1", 1),  # mismatched alphas
        ("1.2.3b1", "1.2.3a1", 1),  # alpha vs beta
        ("1.2.3b1", "1.2.3b1", 0),  # equal betas
        # alpha, beta, and dev versions take a back-seat  to
        # mistmatched MAJOR.MINOR.PATCH
        ("1.2.5a1", "1.2.4b1", 1),  # alpha vs beta
        ("1.2.5a1", "1.2.4-dev", 1),  # alpha vs dev
        ("1.2.5", "1.2.4-dev", 1),  # dev vs non-pre
    ],
)
def test_compare_versions(left, right, expect_result):
    assert compare_versions(left, right) == expect_result
    assert compare_versions(right, left) == -expect_result
