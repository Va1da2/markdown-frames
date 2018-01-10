# Markdown-frames

This is small utility package that makes unit tests for data related tasks readable and documentation-like.

## Motivation

While working with Apache Spark (Pyspark to be precise) to make a data application I encountered a problem
that it wasn't a nice way how to perform unit tests and them being descriptive enough so that not-coders 
could inspect them to get the intuition what the part of the code does OR they even could define a test
themselves. 

So instead of having to define a Spark DataFrame like this:
```
schema = ["user_id", "even_type", "item_id", "event_time", "country", "dt"]
input_df = spark.createDataFrame([
    (123456, 'page_view', None, "2017-12-31 23:50:50", "uk", "2017-12-31"),
    (123456, 'item_view', 68471513, "2017-12-31 23:50:55", "uk", "2017-12-31")], 
    schema)
```

I want to define it as a docstring:
```
input_markdown = """
    |  user_id   |  even_type  | item_id  |    event_time       | country  |     dt      |
    |   bigint   |   string    |  bigint  |    timestamp        |  string  |   string    |
    | ********** | *********** | ******** | ******************* | ******** |************ |
    |   123456   |   page_view |   NULL   | 2017-12-31 23:50:50 |   uk     | 2017-12-31  |
    |   123456   |   item_view | 68471513 | 2017-12-31 23:50:55 |   uk     | 2017-12-31  |
    """

input_df = spark_df(input_markdown, spark)
```

This not only gives better view of the data that you use in unittest for yourself, but also for other people who will try to understand your code.
Also, in this way, you can easily share test with your python (code) challenged coleagues to allow them better understand what the specific function is doing.
In this way, these coleagues might even write you a unit test that will catch an edge case that you did not thought of. 


If you want to see how they are used, head to the `examples` folder.

## Environment setup

Please refer to the manual [here](ENVIRONMENT_SETUP.md)
