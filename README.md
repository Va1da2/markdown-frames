# Markdown-frames

This is small utility package that makes unit tests for data related tasks readable and documentation-like.

## Motivation

While working with Apache Spark (Pyspark to be precise) to make a data application I encountered a problem
that it wasn't a nice way how to perform unit tests and them being descriptive enough so that not-coders 
could inspect them to get the intuition what the part of the code does OR they even could define a test
themselves. 

So instead of having to define a Spark DataFrame like this:
```
schema = ["column_1", "column_2"]
input = spark.createDataFrame([(1, 'first'), (2, 'second')], schema)
```

I want to define it as a docstring:
```
input = """
    |  column_1  |  column_2  |
    |  integer   |   string   |
    | ---------- | ---------- |
    |     1      |   first    |
    |     2      |   second   |
    """
```

## TO DO:

* make initial implementation of markdown table parsing (without `array` or `map`) 
* add `array` and `map` support
* dependencies description (how to install pyspark)
* make it to a package with `setup.py`
