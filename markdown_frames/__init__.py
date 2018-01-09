"""
Module contains functions that given markdown table
in docstring produces spark or pandas table.
"""
from markdown_frames.spark_dataframe import spark_df
from markdown_frames.pandas_dataframe import pandas_df

from markdown_frames.version import __version__

__all__ = ["pandas_dataframe", "spark_dataframe"]
