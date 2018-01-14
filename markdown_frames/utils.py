"""
Functions that are used for spark dataframe and pandas dataframe.

Functions provided:
* make_table
* get_column_names_types
* get_data_from_table
* get_python_type
"""
from typing import List, Any, Optional
from datetime import datetime
from ast import literal_eval
import re

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
    row_values = filter(lambda s: s.strip() != '', row_string.split('|'))
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

def get_column_names_types(table: List[List[str]]) -> List[List[str]]:
    """
    Given a table in list of lists representation, output the lists for
    column names and column types.
    :param table: markdown table representation as list of lists (rows)
    :return: list of lists representaion of data in provided table
    """
    # Check if types are provided?
    column_names, types = table[:2]
    return [column_names, types]

def get_data_from_table(table: List[List[str]]) -> List[List[str]]:
    """
    Given markdown table split into list of rows (lists), find a
    index of list from which data starts and return only the data part
    of table.
    :param table: markdown table representation as list of lists (rows)
    :return: index of the row where data starts
    """
    # The thing to check is if data is seprated from
    # column descriptions (name & type) or not by separator (`-`)
    element_3_1 = table[2][0]
    if element_3_1 == element_3_1[0] * len(element_3_1):
        data = table[3:]
    else:
        data = table[2:]

    return data

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

def get_array_inside_type(column_type: str) -> str:
    """
    Given column_type string, extract
    array inside pattern using regex.
    :param column_type: string description of
        column_type.
    :returns: Str
    """
    matchObj = re.match("array\<(.*)\>", column_type)
    if matchObj:
        return matchObj.group(1).strip()
    return None