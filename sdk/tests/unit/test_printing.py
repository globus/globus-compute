import io
from contextlib import redirect_stdout

import pytest
from globus_compute_sdk.sdk.utils.printing import print_table


@pytest.mark.parametrize(
    "table_input",
    [
        [
            [],
            [
                ["a", "b", "c", "d"],
                ["1", "2", "3"],
            ],
            [
                "| Column 1 | Column 2 | Column 3 | Column 4 |",
                "| a        | b        | c        | d        |",
                "| 1        | 2        | 3        |          |",
            ],
        ],
        [
            ["a", "b", "c"],
            [
                [
                    "123456789 123456789 123456789 1234567890",
                ],
                ["a", "b", "d"],
                ["1", "2", "3"],
            ],
            [
                "|                    a                     | b | c |",
                "| 123456789 123456789 123456789 1234567890 |   |   |",
                "| a                                        | b | d |",
            ],
        ],
        [
            ["a", "b", "c"],
            [
                ["1", "2", "3", "4"],
                ["a", "b"],
            ],
            [
                "| a | b | c | Column 1 |",
                "| 1 | 2 | 3 | 4        |",
                "| a | b |   |          |",
            ],
        ],
    ],
)
def test_print_table(table_input):
    f = io.StringIO()
    with redirect_stdout(f):
        header, rows, rows_out = table_input
        print_table(header, rows)
        s = f.getvalue().split("\n")
        assert rows_out[0] == s[1]
        assert rows_out[1] == s[3]
        assert rows_out[2] == s[5]
