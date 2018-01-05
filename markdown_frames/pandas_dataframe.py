"Function that parse markdown table to Pandas DataFrame."
import pandas as pd

from markdown_frames.utils import (
    make_table,
    get_data_starting_index,
    get_python_type
    )

def pandas_df(markdown_table: str) -> DataFrame:
    """
    Given SparkSessin and markdown representation of your data,
    function returns a Spark DataFrame with specified types.
    :param markdown_table: markdown representation of input data.
    :param spark: SparkSession
    :return: DataFrame with data and schema specified.
    """
    table = make_table(markdown_table)
    column_names = table[0]
    types = table[1]
    starting_index = get_data_starting_index(table)
    output_table = []
    for row in table[starting_index:]:
        output_table.append(tuple(map(get_python_type, zip(row, types))))

    return pd.DataFrame.form_records(output_table, column_names)