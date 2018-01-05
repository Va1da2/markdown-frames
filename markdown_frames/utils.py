"Functions that are used for spark dataframe and pandas dataframe."
from typing import List, Any, Optional
from datetime import datetime
from ast import literal_eval

from markdown_frames.type_definitions import (
    NULL,
    STRING,
    INTS,
    FLOATS,
    TIMESTAMP
    )


def _make_columns(row_string: str) -> List[str]:
    """
    Provided with input string of single table row,
    return a formated values for that row.
    :param row_string: string that represent one row for
        input table
    :return: list of formated values in the table row
    """
    cleaned_row = row_string.strip()
    row_values = filter(lambda s: s.strip() != '', cleaned_row.split('|'))
    return list(map(lambda s: s.strip().lower(), row_values))

def make_table(markdown_table: str) -> List[List[str]]:
    """
    Given markdown table produce a list of lists - table. Still strings.
    :param markdown_table: table in markdown format
    :return: list of lists with rows of data (still in str format)
    """
    table = map(_make_columns, markdown_table.split('\n'))
    filtered_table = filter(lambda x: x, table)

    return list(filtered_table)

def get_python_type(value_type: List[str]) -> Optional[Any]:
    """
    Guven a tuple of (str(value), type) return a value in correct
    python type.
    :param value_type: tuple made by zip'ing `row` list with `types` list
    :return: value in correct python type
    """
    value, type_ = value_type
    if value not in NULL:
        if type_ in STRING:
            return str(value)
        elif type_ in INTS:
            return int(value)
        elif type_ in FLOATS:
            return float(value)
        elif type_ in TIMESTAMP:
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        else:
            return literal_eval(value)
    else:
        return None

def get_data_starting_index(table: List[Any]) -> int:
    """
    Given markdown table split into list of rows (lists), find a
    index of list from which data starts. This might be index 2, if 
    user did not provide separator (e.g. '----') or index 3 if he did.
    :param table: markdown table representation as list of lists (rows)
    :return: index of the row where data starts
    """
    #check the first element of 3rd row
    element_3_1 = table[2][0]
    if element_3_1 == '-' * len(element_3_1):
        index = 3
    else:
        index = 2

    return index