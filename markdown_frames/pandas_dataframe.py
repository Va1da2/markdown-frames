"""
Function that parse markdown table to Pandas DataFrame.

Functions provided:
* pandas_df
"""
import pandas as pd

from markdown_frames.utils import (
    make_table,
    get_column_names_types,
    get_data_from_table,
    get_python_type
)


def pandas_df(markdown_table: str) -> pd.DataFrame:
    """
    Given markdown representation of your data,
    function returns a Pandas DataFrame with specified types.
    :param markdown_table: markdown representation of input data.
    :return: DataFrame with data.
    """
    table = make_table(markdown_table)
    column_names, types = get_column_names_types(table)
    table_data = get_data_from_table(table)
    output_table = []
    for row in table_data:
        output_table.append(tuple(map(get_python_type, zip(row, types))))

    return pd.DataFrame(output_table, columns=column_names)
