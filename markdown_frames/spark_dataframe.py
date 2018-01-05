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

from typing import List

from markdown_frames.utils import (
    make_table,
    get_data_starting_index,
    get_python_type
    )
from markdown_frames.type_definitions import (
    STRING,
    INTEGER,
    BIG_INTEGER,
    FLOAT,
    DOUBLE,
    TIMESTAMP,
    )


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
        elif column_type in STRING:
            return StringType()

    def struct_field(name_type):
        return StructField(name_type[0], types_mapping(name_type[1]))

    spark_structs = map(struct_field, zip(column_names, column_types))

    return StructType(list(spark_structs))

def spark_df(markdown_table: str, spark: SparkSession) -> DataFrame:
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
    spark_struct = _get_spark_struct(column_names, types)

    return spark.createDataFrame(output_table, spark_struct)
