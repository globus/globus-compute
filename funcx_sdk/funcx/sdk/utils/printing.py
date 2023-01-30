from __future__ import annotations

import shutil

import texttable


def print_table(
    headers: list,
    table_rows: list,
    output_file=None,
) -> None:
    """
    A thin wrapper around texttable that adds optional file as output.
      Also pads header/rows to equalize width with padding of cells

     :param headers: Headings for the table.  If not given, will be auto
                      generated with generic 'column #' text
     :param table_rows: Rows of the table, each a list of cells
     :param output_file: Output file like object (stream).  If None, use stdout
    """
    # Calculate max columns
    max_columns = len(headers)
    for row in table_rows:
        if len(row) > max_columns:
            max_columns = len(row)

    if len(headers) < max_columns:
        headers = headers + [f"Column {i+1}" for i in range(max_columns - len(headers))]

    table = texttable.Texttable()
    table.header(headers)

    # make table terminal wide.  (Not setting a min width as done elsewhere)
    table.set_max_width(shutil.get_terminal_size().columns)

    for row in table_rows:
        table.add_row(row + [""] * (max_columns - len(row)))

    print(table.draw(), file=output_file)
