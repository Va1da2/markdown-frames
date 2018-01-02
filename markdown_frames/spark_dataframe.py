"Function that parse markdown table to Apache Spark (PySpark) DataFrame."
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


INTEGER = ['int', 'integer']
BIG_INTEGER = ['bigint', 'big int', 'big integer']
FLOAT = ['float']
DOUBLE = ['double', 'decimal']
TIMESTAMP = ['timestamp', 'time stamp']
NULL = ['null', 'none', 'na', 'nan']


def _make_columns(row_string):
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

def _make_table(markdown_table):
    """
    Given markdown table representation produce a 
    """
    return list(map(_make_columns, markdown_table.split('\n')))

def _get_type(input_type, inp):
    """Return input in desired type.

    Args:
        input_type (str): desired data type of input.
        inp (str): input.

    Returns:
        The input in desired type.
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


def _get_spark_struct(column_names, column_types):
    """Return StructType while constructing spark df.

    Args:
        column_names (list): column names (parsed from docstring df).
        columns_types (list): column types (parsed from docstring df).

    Returns:
        StructType.
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

    structs = []
    for column_name, column_type in zip(column_names, column_types):
        spark_structs.append(StructField(column_name, types_mapping(column_type)))
    return StructType(spark_structs)


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