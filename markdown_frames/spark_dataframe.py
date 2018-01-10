"""
Function that parse markdown table to Apache Spark (PySpark) DataFrame.

Functions provided:
* spark_df
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    IntegerType,
    FloatType,
    DoubleType,
    LongType,
    ShortType,
    TimestampType,
    StringType,
    ArrayType,
    MapType,
    StructType,
    StructField
)

from typing import List, Union, Any

from markdown_frames.utils import (
    make_table,
    get_column_names_types,
    get_data_from_table,
    get_python_type
)
from markdown_frames.type_definitions import (
    STRING,
    INTEGER,
    BIG_INTEGER,
    SMALL_INTEGER,
    FLOAT,
    DOUBLE,
    TIMESTAMP,
)


def spark_df(markdown_table: str, spark: SparkSession) -> DataFrame:
    """
    Given SparkSessin and markdown representation of your data,
    function returns a Spark DataFrame with specified types.
    :param markdown_table: markdown representation of input data.
    :param spark: SparkSession
    :return: DataFrame with data and schema specified.
    """
    table = make_table(markdown_table)
    column_names, types = get_column_names_types(table)
    table_data = get_data_from_table(table)
    output_table = []
    for row in table_data:
        output_table.append(tuple(map(get_python_type, zip(row, types))))
    spark_struct = _get_spark_struct(column_names, types)

    return spark.createDataFrame(output_table, spark_struct)

def _get_spark_struct(column_names: List[str],
                      column_types: List[str]) -> StructType:
    """
    Given column names nad column tapes,
    produces struct type for Spark DataFrame.
    :param column_names: column names in list
    :param columns_types: column types in list
    :returns: StructType.
    """
    def struct_field(name_type):
        return StructField(name_type[0], _types_mapping(name_type[1]))

    spark_structs = map(struct_field, zip(column_names, column_types))

    return StructType(list(spark_structs))

def _types_mapping(column_type: str) -> Any:
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
    elif column_type in SMALL_INTEGER:
        return ShortType()
    elif column_type in STRING:
        return StringType()
    elif _is_map_type(column_type):
        return _map_type(column_type)
    elif _is_array_type(column_type):
        return _array_type(column_type)

def _is_map_type(column_type: str) -> bool:
    """
    Given column_type string returns boolean value
    if given string is for MapType.
    :param column_type: string description of 
        column type
    :return: boolean - MapType or not.
    """
    if column_type.find("map<") == -1:
        return False
    else:
        return True

def _map_type(column_type: str) -> MapType:
    """
    Given column_type string returns MapType
    with the correct (key, value) types.
    :param column_type: string description of
        column_type.
    :returns: MapType
    """
    key, value = list(map(lambda x: x.strip(), column_type[4:-1].split(',')))

    return MapType(_types_mapping(key), _types_mapping(value))

def _is_array_type(column_type: str) -> bool:
    """
    Given column_type string returns boolean value
    if given string is for ArrayType.
    :param column_type: string description of 
        column type
    :return: boolean - ArrayTaype or not.
    """
    if column_type.find("array<") == -1:
        return False
    else:
        return True

def _array_type(column_type: str) -> ArrayType:
    """
    Given column_type string returns ArrayType
    with the correct inside item type.
    :param column_type: string description of
        column_type.
    :returns: ArrayType
    """
    inside = column_type[6:-1].strip()

    return ArrayType(_types_mapping(inside))
