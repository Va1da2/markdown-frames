"Test markdown_frames/spark_dataframe.py functions."
import pytest
import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
    StringType,
    TimestampType,
    MapType,
    ArrayType
    )

from markdown_frames.spark_dataframe import (
    _get_spark_struct,
    spark_df
    )

@pytest.fixture(scope='session')
def spark(request):
    spark = (SparkSession.builder
        .appName('Test spark_dataframe.py functions')
        .master('local[*]')
        .getOrCreate())
    request.addfinalizer(lambda: spark.stop())
    logging.getLogger('py4j').setLevel(logging.WARN)
    return spark


def test__get_spark_struct(spark):
    """
    Test function that given list of column names and another
    list with column types description will return a StrucType
    that represents DatFrame schema.
    """
    input_column_names = ["column1", "column2", "column3", "column4"]
    input_column_types1 = ["int", "float", "integer", "bigint"]
    input_column_types2 = ["decimal", "str", "timestamp", "double"]

    expected_output1 = StructType([
        StructField("column1", IntegerType()),
        StructField("column2", FloatType()),
        StructField("column3", IntegerType()),
        StructField("column4", LongType())
        ])
    expected_output2 = StructType([
        StructField("column1", DoubleType()),
        StructField("column2", StringType()),
        StructField("column3", TimestampType()),
        StructField("column4", DoubleType())
        ])

    output1 = _get_spark_struct(input_column_names, input_column_types1)
    output2 = _get_spark_struct(input_column_names, input_column_types2)

    assert output1 == expected_output1
    assert output2 == expected_output2

def test_spark_df(spark):
    """
    Test function that given markdown representation of 
    your data will return a Spark DataFrame with schema
    specified.
    """
    input_table1 = (
        """
        | column1 | column2 | column3 | column4 |
        |   int   |  string |  float  |  bigint |
        | ******* | ******* | ******* | ******* |
        |   1     |   user1 |   3.14  |  111111 |
        |   2     |   null  |   1.618 |  222222 |
        |   3     |   user4 |   2.718 |  333333 |
        """
        )
    input_table2 = (
        """
        | column1 |        column2      | column3 |  column4   |
        | decimal |       timestamp     |  double |   string   |    
        |  2.22   | 2017-01-01 23:59:59 |  2.333  | 2017-01-01 |
        |  3.33   |         None        |  3.222  | 2017-12-31 |
        |   na    | 2017-12-31 23:59:59 |  4.444  | 2017-12-31 |
        """
        )
    expected_schema1 = StructType([
        StructField("column1", IntegerType()),
        StructField("column2", StringType()),
        StructField("column3", FloatType()),
        StructField("column4", LongType()),
        ])
    expected_table1 = spark.createDataFrame(
        [(1, 'user1', 3.14, 111111),
         (2, None, 1.618, 222222),
         (3, 'user4', 2.718, 333333)],
        expected_schema1)
    epxected_schema2 = StructType([
        StructField("column1", DoubleType()),
        StructField("column2", TimestampType()),
        StructField("column3", DoubleType()),
        StructField("column4", StringType()),
        ])
    expected_table2 = spark.createDataFrame(
        [(2.22, datetime(2017, 1, 1, 23, 59, 59), 2.333, '2017-01-01'),
         (3.33, None, 3.222, '2017-12-31'),
         (None, datetime(2017, 12, 31, 23, 59, 59), 4.444, '2017-12-31')],
        epxected_schema2)

    output_table1 = spark_df(input_table1, spark)
    output_table2 = spark_df(input_table2, spark)

    output_table1.toPandas().equals(expected_table1.toPandas())
    output_table2.toPandas().equals(expected_table2.toPandas())
