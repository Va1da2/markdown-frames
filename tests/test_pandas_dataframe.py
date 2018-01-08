import pytest
import pandas as pd
from datetime import datetime

from markdown_frames.pandas_dataframe import pandas_df

def test_pandas_df():
    """
    Test function that provided with mardown table outputs pandas
    dataframe.
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

    expected_table1 = pd.DataFrame(
        [(1, 'user1', 3.14, 111111),
         (2, None, 1.618, 222222),
         (3, 'user4', 2.718, 333333)],
        columns=["column1", "column2", "column3", "column4"])
    expected_table2 = pd.DataFrame(
        [(2.22, datetime(2017, 1, 1, 23, 59, 59), 2.333, '2017-01-01'),
         (3.33, None, 3.222, '2017-12-31'),
         (None, datetime(2017, 12, 31, 23, 59, 59), 4.444, '2017-12-31')],
        columns=["column1", "column2", "column3", "column4"])

    output1 = pandas_df(input_table1)
    output2 = pandas_df(input_table2)

    assert output1.equals(expected_table1)
    assert output2.equals(expected_table2)