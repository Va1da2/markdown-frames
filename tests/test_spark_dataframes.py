"Test markdown_frames/spark_dataframe.py functions."
import pytest
import logging
from datetime import datetime
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

from markdown_frames.spark_dataframe import (
    _is_map_type,
    _is_array_type,
    _map_type,
    _array_type,
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
    input_table3 = (
        """
        | column1 |        column2       | column3     |  column4   |  
        | decimal | map<str, array<int>> |  array<int> |   string   |    
        |  2.22   | {'key': [1, 2, 3]}   |  [1, 1, 1]  | 2017-01-01 |  
        |  3.33   | {'key': [3, 1, 2]}   |  [1, 2, 2]  | 2017-12-31 | 
        |   na    |         None         |  [5, 5, 5]  | 2017-12-31 |    
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
    epxected_schema3 = StructType([
        StructField("column1", DoubleType()),
        StructField("column2", MapType(StringType(), ArrayType(IntegerType()))),
        StructField("column3", ArrayType(IntegerType())),
        StructField("column4", StringType()),
        ])
    expected_table3 = spark.createDataFrame(
        [(2.22, {'key': [1, 2, 3]}, [1, 1, 1], '2017-01-01'),
         (3.33, {'key': [3, 1, 2]}, [1, 2, 2], '2017-12-31'),
         (None, None, [5, 5, 5], '2017-12-31')],
        epxected_schema3)

    output_table1 = spark_df(input_table1, spark)
    output_table2 = spark_df(input_table2, spark)
    output_table3 = spark_df(input_table3, spark)

    output_table1.toPandas().equals(expected_table1.toPandas())
    output_table2.toPandas().equals(expected_table2.toPandas())
    output_table3.toPandas().equals(expected_table3.toPandas())

def test__is_array_type():
    """
    Test function that given a colum description returns if this
    column is of ArrayType.
    """
    column1 = "array<int>"
    column2 = "array<array<str>>"
    column3 = "aray<integer>"
    column4 = "array int"

    expected1 = True
    expected2 = True
    expected3 = False
    expected4 = False

    output1 = _is_array_type(column1)
    output2 = _is_array_type(column2)
    output3 = _is_array_type(column3)
    output4 = _is_array_type(column4)

    assert output1 == expected1
    assert output2 == expected2
    assert output3 == expected3
    assert output4 == expected4

def test__is_map_type():
    """
    Test function that given a colum description returns if this
    column is of MapType.
    """
    column1 = "map<int, str>"
    column2 = "map"
    column3 = "str"

    expected1 = True
    expected2 = False
    expected3 = False

    output1 = _is_map_type(column1)
    output2 = _is_map_type(column2)
    output3 = _is_map_type(column3)

    assert output1 == expected1
    assert output2 == expected2
    assert output3 == expected3

def test__array_type_ok():
    """
    Test function that given a colum description returns if this
    column is of ArrayType (all columns are of valid description).
    """
    column1 = "array<int>"
    column2 = "array<array<str>>"
    column3 = "array<double>"
    column4 = "array<float>"

    expected1 = ArrayType(IntegerType())
    expected2 = ArrayType(ArrayType(StringType()))
    expected3 = ArrayType(DoubleType())
    expected4 = ArrayType(FloatType())

    output1 = _array_type(column1)
    output2 = _array_type(column2)
    output3 = _array_type(column3)
    output4 = _array_type(column4)

    assert output1 == expected1
    assert output2 == expected2
    assert output3 == expected3
    assert output4 == expected4

def test__map_type_ok():
    """
    Test function that given a MapType
    column description would return a 
    valid column type for struct field.
    """
    column1 = "map<str, int>"
    column2 = "map<int, array<str>>"
    column3 = "map<float, double>"
    
    expected1 = MapType(StringType(), IntegerType())
    expected2 = MapType(IntegerType(), ArrayType(StringType()))
    expected3 = MapType(FloatType(), DoubleType())

    output1 = _map_type(column1)
    output2 = _map_type(column2)
    output3 = _map_type(column3)

    assert output1 == expected1
    assert output2 == expected2
    assert output3 == expected3

def test__get_spark_struct():
    """
    Test function that given list of column names and another
    list with column types description will return a StrucType
    that represents DatFrame schema.
    """
    input_column_names = ["column1", "column2", "column3", "column4"]
    input_column_types1 = ["int", "float", "integer", "bigint"]
    input_column_types2 = ["decimal", "str", "timestamp", "double"]
    input_column_types3 = ["map<string, array<int>>", "double", "str", "array<array<float>>"]

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

    expected_output3 = StructType([
        StructField("column1", MapType(StringType(), ArrayType(IntegerType()))),
        StructField("column2", DoubleType()),
        StructField("column3", StringType()),
        StructField("column4", ArrayType(ArrayType(FloatType())))
        ])

    output1 = _get_spark_struct(input_column_names, input_column_types1)
    output2 = _get_spark_struct(input_column_names, input_column_types2)
    output3 = _get_spark_struct(input_column_names, input_column_types3)

    assert output1 == expected_output1
    assert output2 == expected_output2
    assert output3 == expected_output3
