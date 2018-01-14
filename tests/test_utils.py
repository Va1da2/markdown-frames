"Test markdown_frames/utils.py functins."
import pytest
from datetime import datetime

from markdown_frames.utils import (
    _make_columns,
    make_table,
    get_column_names_types,
    get_data_from_table,
    get_python_type,
    get_array_inside_type
    )


def test__make_columns():
    """
    Test function that give an string line of one row
    produces a column form that row.
    """
    column_names1 = "| col1 | col2 | col3 |"
    column_names2 = "  | Col_1 | COL_2 | col_3 |  "
    types1 = " | str | int | bigINT |     decimal |  FLOAT  |"
    types2 = " | str | map<str, array<int>> |"
    data1 = " | string    |  12254 | 1025.154 | "
    data2 = "|111| 111  |   11.11 |"
    data3 = " | user_12345 | {'smth': 1} |  "
    data4 = "|index| [[11, 11, 11],[1, 1, 3]] |"

    expected_column_names1 = ["col1", "col2", "col3"]
    expected_column_names2 = ["col_1", "col_2", "col_3"]
    expected_types1 = ["str", "int", "bigint", "decimal", "float"]
    expected_types2 = ["str", "map<str, array<int>>"]
    expected_data1 = ["string", "12254", "1025.154"]
    expected_data2 = ["111", "111", "11.11"]
    expected_data3 = ["user_12345", "{'smth': 1}"]
    expected_data4 = ["index", "[[11, 11, 11],[1, 1, 3]]"]

    output_column_names1 = _make_columns(column_names1)
    output_column_names2 = _make_columns(column_names2)
    output_types1 = _make_columns(types1)
    output_types2 = _make_columns(types2)
    output_data1 = _make_columns(data1)
    output_data2 = _make_columns(data2)
    output_data3 = _make_columns(data3)
    output_data4 = _make_columns(data4)

    assert output_column_names1 == expected_column_names1
    assert output_column_names2 == expected_column_names2
    assert output_types1 == expected_types1
    assert output_types2 == expected_types2
    assert output_data1 == expected_data1
    assert output_data2 == expected_data2
    assert output_data3 == expected_data3
    assert output_data4 == expected_data4

def test_make_table():
    """
    Test function that given a markdown representation of your table
    will output list of lists representation of your table.
    """
    table1 = """
        | column1 | column2 | column3 |
        |   str   |  int    |  float  |
        | ------- | ------- | ------- |
        |  hello  |   1     |  1.11   |
        """
    table2 = (
        """
        | column1 | column2 | column3 |
        |   str   |  int    |  float  |  
        | ------- | ------- | ------- |  
        |  hello  |   1     |  1.11   |   
        """)
    table3 = (
        """
        | column1 | column2 |    column3      |
        |   str   |  int    |  map<str, int>  |   
        |  hello  |   1     |  {'asdd': 1}    |   
        """)

    expected_table1 = [
        ['column1', 'column2', 'column3'],
        ['str', 'int', 'float'],
        ['-------', '-------', '-------'],
        ['hello', '1', '1.11']
        ]
    expected_table2 = [
        ['column1', 'column2', 'column3'],
        ['str', 'int', 'float'],
        ['-------', '-------', '-------'],
        ['hello', '1', '1.11']
        ]
    expected_table3 = [
        ['column1', 'column2', 'column3'],
        ['str', 'int', 'map<str, int>'],
        ['hello', '1', "{'asdd': 1}"]
        ]

    output_table1 = make_table(table1)
    output_table2 = make_table(table2)
    output_table3 = make_table(table3)

    assert output_table1 == expected_table1
    assert output_table2 == expected_table2
    assert output_table3 == expected_table3

def test_get_column_names_types():
    """
    Test function that given a table (list of lists) returns list of column names
    and types.
    """
    table1 = [
        ['column1', 'column2', 'column3'],
        ['str', 'int', 'float'],
        ['-------', '-------', '-------'],
        ['hello', '1', '1.11']
        ]
    table2 = [
        ['column1', 'column2', 'column3'],
        ['str', 'int', 'map<str, int>'],
        ['hello', '1', "{'asdd': 1}"]
        ]

    expected_columns1 = ['column1', 'column2', 'column3']
    expected_columns2 = ['column1', 'column2', 'column3']
    expected_types1 = ['str', 'int', 'float']
    expected_types2 = ['str', 'int', 'map<str, int>']

    output_columns1, output_types1 = get_column_names_types(table1)
    output_columns2, output_types2 = get_column_names_types(table2)

    assert output_columns1 == expected_columns1
    assert output_types1 == expected_types1
    assert output_columns2 == expected_columns2
    assert output_types2 == expected_types2

def test_get_data_from_table():
    """
    Test function that given a table (list of lists) returns only data
    part of the table.
    """
    table1 = [
        ['column1', 'column2', 'column3'],
        ['str', 'int', 'float'],
        ['-------', '-------', '-------'],
        ['hello', '1', '1.11']
        ]
    table2 = [
        ['column1', 'column2', 'column3'],
        ['str', 'int', 'map<str, int>'],
        ['hello', '1', "{'asdd': 1}"]
        ]
    table3 = [
        ['column1', 'column2', 'column3'],
        ['str', 'int', 'map<str, int>'],
        ['*******', '******', '*******'],
        ['hello', '1', "{'asdd': 1}"]
        ]
    table4 = [
        ['column1', 'column2', 'column3'],
        ['str', 'int', 'map<str, int>'],
        ['______', '______', '______'],
        ['hello', '1', "{'asdd': 1}"]
        ]

    expected_data_table1 = [['hello', '1', '1.11']]
    expexted_data_table2 = [['hello', '1', "{'asdd': 1}"]]
    expexted_data_table3 = [['hello', '1', "{'asdd': 1}"]]
    expexted_data_table4 = [['hello', '1', "{'asdd': 1}"]]

    output_data_table1 = get_data_from_table(table1)
    output_data_table2 = get_data_from_table(table2)
    output_data_table3 = get_data_from_table(table3)
    output_data_table4 = get_data_from_table(table4)

    assert output_data_table1 == expected_data_table1
    assert output_data_table2 == expexted_data_table2
    assert output_data_table3 == expexted_data_table3
    assert output_data_table4 == expexted_data_table4

def test_get_python_type():
    """
    Test fucntion that give a type description returns input
    value in correct python type.
    """
    input1 = ("null", "int")
    input2 = ("1", "int")
    input3 = ("name", "str")
    input4 = ("1.35", "float")
    input5 = ("1.35", "decimal")
    input6 = ("2017-01-01 23:59:59", "timestamp")
    input7 = ("[1, 2, 3]", "array<int>")
    input8 = ("{'key': 1}", "map<str, int>")

    expected1 = None
    expected2 = 1
    expected3 = "name"
    expected4 = 1.35
    expected5 = 1.35
    expected6 = datetime(2017, 1, 1, 23, 59, 59)
    expected7 = [1, 2, 3]
    expected8 = {'key': 1}

    output1 = get_python_type(input1)
    output2 = get_python_type(input2)
    output3 = get_python_type(input3)
    output4 = get_python_type(input4)
    output5 = get_python_type(input5)
    output6 = get_python_type(input6)
    output7 = get_python_type(input7)
    output8 = get_python_type(input8)

    assert output1 == expected1
    assert output2 == expected2
    assert output3 == expected3
    assert output4 == expected4
    assert output5 == expected5
    assert output6 == expected6
    assert output7 == expected7
    assert output8 == expected8

def test_get_array_type():
    """
    Test fucntion that given array type string pattern
    extract inside type
    """
    input1 = "array<int>"
    input2 = "array<int >"
    input3 = "array<array<str>>"

    expected1 = "int"
    expected2 = "int"
    expected3 = "array<str>"

    output1 = get_array_inside_type(input1)
    output2 = get_array_inside_type(input2)
    output3 = get_array_inside_type(input3)

    assert output1 == expected1
    assert output2 == expected2
    assert output3 == expected3