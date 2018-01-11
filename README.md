# Markdown-frames

This is small utility package that makes unit tests for data related tasks readable and documentation-like.

## Motivation

While working on Apache Spark (Pyspark to be precise) applications @Exacaster (my current employer) I encountered a problem
that there were not a convenient way to perform unit tests for the modules we write. Sure, you can 
create a DataFrame like this
```
schema = ["user_id", "even_type", "item_id", "event_time", "country", "dt"]
input_df = spark.createDataFrame([
    (123456, 'page_view', None, datetime(2017,12,31,23,50,50), "uk", "2017-12-31"),
    (123456, 'item_view', 68471513, datetime(2017,12,31,23,50,55), "uk", "2017-12-31")], 
    schema)
```
And then pass it to the function and check the output in the same way, but this even with two rows
and six columns it is barely readable. Instead I wanted something that would be better to write and
read and even could a slight advantage of getting your test cases from someone that actually uses 
your output - analysts. Also during my time @Vinted brilliant team of DWH developers introduced me to 
Spark jobs testing through `cucumber`, where all data was described via markdown tables. That's what I
wanted to do - to describe data not like presented above, but like this:
```
    |  user_id   |  even_type  | item_id  |    event_time       | country  |     dt      |
    |   bigint   |   string    |  bigint  |    timestamp        |  string  |   string    |
    | ********** | *********** | ******** | ******************* | ******** |************ |
    |   123456   |   page_view |   NULL   | 2017-12-31 23:50:50 |   uk     | 2017-12-31  |
    |   123456   |   item_view | 68471513 | 2017-12-31 23:50:55 |   uk     | 2017-12-31  |
```
This is a lot more readable. It is clear what types are in which columns, and you can get a better
fieling of what function expects and what it outputs when executed.

If you have suggestions or questions - just open an issue. I would like to make this little package usefull
(it is in our company) to more people.

## Usage

After instalation - a simple `pip install markdown_frames` (I assume you are in `pyspark` REPL or have a `SparkSession` defined in your test suite)
```
from markdown_frames import spark_df

input_table = """
    |  user_id   |  even_type  | item_id  |    event_time       | country  |     dt      |
    |   bigint   |   string    |  bigint  |    timestamp        |  string  |   string    |
    | ********** | *********** | ******** | ******************* | ******** |************ |
    |   123456   |   page_view |   NULL   | 2017-12-31 23:50:50 |   uk     | 2017-12-31  |
    |   123456   |   item_view | 68471513 | 2017-12-31 23:50:55 |   uk     | 2017-12-31  |
    """
input_df = spark_df(input_table, spark)

input_df.show()
# output
#+-------+---------+--------+-------------------+-------+----------+
#|user_id|even_type| item_id|         event_time|country|        dt|
#+-------+---------+--------+-------------------+-------+----------+
#| 123456|page_view|    null|2017-12-31 23:50:50|     uk|2017-12-31|
#| 123456|item_view|68471513|2017-12-31 23:50:55|     uk|2017-12-31|
#+-------+---------+--------+-------------------+-------+----------+

input_df.dtypes
# output
#[('user_id', 'bigint'), ('even_type', 'string'), ('item_id', 'bigint'), ('event_time', 'timestamp'), ('country', 'string'), ('dt', 'string')]

input_df.schema
# output
# StructType(List(StructField(user_id,LongType,true),StructField(even_type,StringType,true),StructField(item_id,LongType,true),StructField(event_time,TimestampType,true),StructField(country,StringType,true),StructField(dt,StringType,true)))
```

### PySpark types supported

For now these PySpark types are supported:

* `StringType`
* `TimestampType`
* `DoubleType`
* `FloatType`
* `IntegerType`
* `LongType`
* `ShortType`
* `ArrayType`
* `MapType`

DecimalType should be added soon.

## Environment setup

Please refer to the manual for the instalation guadelines [here](ENVIRONMENT_SETUP.md)
