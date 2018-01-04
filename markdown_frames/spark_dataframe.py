"Function that parse markdown table to Apache Spark (PySpark) DataFrame."
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    IntegerType,
    FloatType,
    DoubleType,
    LongType,
    TimestampType,
    StringType,
    StructType,
    StructField
)

from itertools import filterfalse
from typing import List, Union, Any
from datetime import datetime


INTEGER = ['int', 'integer']
BIG_INTEGER = ['bigint', 'big int', 'big integer']
FLOAT = ['float']
DOUBLE = ['double', 'decimal']
TIMESTAMP = ['timestamp', 'time stamp']
NULL = ['null', 'none', 'na', 'nan']


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

def _make_table(markdown_table: str) -> List[List[str]]:
    """
    Given markdown table produce a list of lists - table. Still strings.
    :param markdown_table: table in markdown format
    :return: list of lists with rows of data (still in str format)
    """
    table = map(_make_columns, markdown_table.split('\n'))
    filtered_table = filter(lambda x: x, table)

    return list(filtered_table)

def _get_type(inp: str, input_type: str) -> Union[int, float, str, datetime]:
    """Return input in desired type.
    :param input: input value
    :param input_type: desired data type of input value
    :returns: input value in desired data type
    """
    if inp not in NULL:
        if input_type in INTEGER or input_type in BIG_INTEGER:
            return int(inp)
        elif input_type in FLOAT or input_type in DOUBLE:
            return float(inp)
        elif input_type in TIMESTAMP:
            return datetime.strptime(inp, "%Y-%m-%d %H:%M:%S")
        else:
            return str(inp)


def _get_spark_struct(column_names: List[str],
                      column_types: List[str]) -> StructType:
    """
    Given column names nad column tapes,
    produces struct type for Spark DataFrame.
    :param column_names: column names in list
    :param columns_types: column types in list
    :returns: StructType.
    """
    def types_mapping(column_type):
        if column_type in INTEGER:
            return IntegerType()
        elif column_type in FLOAT:
            return FloatType()
        elif column_type in DOUBLE:
            return DoubleType()
        elif column_type in TIMESTAMP:
            return TimestampType()
        elif column_type in BIG_INTEGER:
            return LongType()
        else:
            return StringType()

    def struct_field(name_type):
        return StructField(name_type[0], types_mapping(name_type[1]))

    spark_structs = map(struct_field, zip(column_names, column_types))

    return StructType(list(spark_structs))

def _get_data_starting_index(table: List[Any]) -> int:
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

def spark_df(markdown_table: str, spark: SparkSession) -> DataFrame:
    """
    Given SparkSessin and markdown representation of your data,
    function returns a Spark DataFrame with specified types.
    :param markdown_table: markdown representation of input data.
    :param spark: SparkSession
    :return: DataFrame with data and schema specified.
    """
    table = _make_table(markdown_table)
    column_names = table[0]
    types = table[1]
    starting_index = _get_data_starting_index(table)



def parse_table_to_spark_df(spark, input_table):
    """Take a multiline comment (docstring like this one) and
    convert it to the spark DataFrame.

    Input table should like like this:

| first_col | second_col | third_col |
    |    int    |   string   |   float   |
    |-----------|------------|-----------|
    |         3 | haha       |       4.6 |
    Separator (----) may be used or not, parse_table function can
    handle this.

    Returns:
        A spark DataFrame.
    """
    input_list = [e for e in input_table.split('|') if e != ' ' * len(e)]
    new_lines_index = [i for i, x in enumerate(input_list) if x[:1] == '\n']
    if new_lines_index[0] != 0:
        new_lines_index.insert(0, 0)

    if new_lines_index[-1] != len(input_list)-1:
        new_lines_index.append(len(input_list))
    column_names = []
    types = []
    table = []

    for i, ind in enumerate(new_lines_index):
        if i == 0:
            for el in input_list[new_lines_index[i]+1 : new_lines_index[i+1]]:
                column_names.append(el.strip())
        elif i == 1:
            for el in input_list[new_lines_index[i]+1 : new_lines_index[i+1]]:
                types.append(el.strip())
        else:
            row_of_values = []
            check = True
            if i == len(new_lines_index)-1:
                check = False
            elif input_list[new_lines_index[i]+1].strip() == '-' * len(input_list[new_lines_index[i]+1].strip()):
                check = False
            if check:
                for j, el in enumerate(input_list[new_lines_index[i]+1 : new_lines_index[i+1]]):
                    value_to_append = _get_type(types[j], el.strip())
                    row_of_values.append(value_to_append)
                table.append(row_of_values)
    spark_struct = _get_spark_struct(column_names, types)
    return spark.createDataFrame(table, spark_struct)