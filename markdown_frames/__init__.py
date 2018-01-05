"Module contains functions that given markdown table in docstring produces spark or pandas table."
from markdown_frames.spark_dataframe import *
from markdown_frames.pandas_dataframe import *

__all__ = ["pandas_dataframe", "spark_dataframe"]