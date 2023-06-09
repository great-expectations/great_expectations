from __future__ import annotations

import warnings
from typing import Callable, Iterator

import pandas as pd

from great_expectations.compatibility import sqlalchemy
from great_expectations.compatibility.not_imported import (
    is_version_greater_or_equal,
    is_version_less_than,
)
from great_expectations.warnings import (
    warn_pandas_less_than_2_0_and_sqlalchemy_greater_than_or_equal_2_0,
)


def execute_pandas_reader_fn(
    reader_fn: Callable, reader_options: dict
) -> pd.DataFrame | list[pd.DataFrame]:
    """Suppress warnings while executing the pandas reader functions.

    If pandas version is below 2.0 and sqlalchemy installed then we suppress
    the sqlalchemy 2.0 warning and raise our own warning. pandas does not
    support sqlalchemy 2.0 until version 2.0 (see https://pandas.pydata.org/docs/dev/whatsnew/v2.0.0.html#other-enhancements)

    Args:
        reader_fn: Reader function to execute.
        reader_options: Options to pass to reader function.

    Returns:
        dataframe or list of dataframes
    """
    if is_version_less_than(pd.__version__, "2.0.0"):
        if sqlalchemy.sqlalchemy and is_version_greater_or_equal(
            sqlalchemy.sqlalchemy.__version__, "2.0.0"
        ):
            warn_pandas_less_than_2_0_and_sqlalchemy_greater_than_or_equal_2_0()
        with warnings.catch_warnings():
            # Note that RemovedIn20Warning is the warning class that we see from sqlalchemy
            # but using the base class here since sqlalchemy is an optional dependency and this
            # warning type only exists in sqlalchemy < 2.0.
            warnings.filterwarnings(action="ignore", category=DeprecationWarning)
            reader_fn_result: pd.DataFrame | list[pd.DataFrame] = reader_fn(
                **reader_options
            )
    else:
        reader_fn_result = reader_fn(**reader_options)
    return reader_fn_result


def pandas_read_sql(sql, con, **kwargs) -> pd.DataFrame | Iterator[pd.DataFrame]:
    """Suppress deprecation warnings while executing the pandas read_sql function.

    Note this only passes params straight to pandas read_sql method, please
    see the pandas documentation
    (currently https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html)
    for more information on this method.

    If pandas version is below 2.0 and sqlalchemy installed then we suppress
    the sqlalchemy 2.0 warning and raise our own warning. pandas does not
    support sqlalchemy 2.0 until version 2.0 (see https://pandas.pydata.org/docs/dev/whatsnew/v2.0.0.html#other-enhancements)

    Args:
        sql: str or SQLAlchemy Selectable (select or text object)
        con: SQLAlchemy connectable, str, or sqlite3 connection
        **kwargs: Other keyword arguments, not enumerated here since they differ
            between pandas versions.

    Returns:
        dataframe
    """
    if is_version_less_than(pd.__version__, "2.0.0"):
        if sqlalchemy.sqlalchemy and is_version_greater_or_equal(
            sqlalchemy.sqlalchemy.__version__, "2.0.0"
        ):
            warn_pandas_less_than_2_0_and_sqlalchemy_greater_than_or_equal_2_0()
        with warnings.catch_warnings():
            # Note that RemovedIn20Warning is the warning class that we see from sqlalchemy
            # but using the base class here since sqlalchemy is an optional dependency and this
            # warning type only exists in sqlalchemy < 2.0.
            warnings.filterwarnings(action="ignore", category=DeprecationWarning)
            return_value = pd.read_sql(sql=sql, con=con, **kwargs)
    else:
        return_value = pd.read_sql(sql=sql, con=con, **kwargs)
    return return_value


def pandas_read_sql_query(sql, con, execution_engine, **kwargs) -> pd.DataFrame:
    """Suppress deprecation warnings while executing the pandas read_sql_query function.

    Note this only passes params straight to pandas read_sql_query method, please
    see the pandas documentation
    (currently https://pandas.pydata.org/docs/reference/api/pandas.read_sql_query.html)
    for more information on this method.

    If pandas version is below 2.0 and sqlalchemy installed then we suppress
    the sqlalchemy 2.0 warning and raise our own warning. pandas does not
    support sqlalchemy 2.0 until version 2.0 (see https://pandas.pydata.org/docs/dev/whatsnew/v2.0.0.html#other-enhancements)

    Args:
        sql: str or SQLAlchemy Selectable (select or text object)
        con: SQLAlchemy connectable, str, or sqlite3 connection
        **kwargs: Other keyword arguments, not enumerated here since they differ
            between pandas versions.

    Returns:
        dataframe
    """
    if (
        sqlalchemy.sqlalchemy
        and is_version_greater_or_equal(sqlalchemy.sqlalchemy.__version__, "2.0.0")
        and is_version_less_than(pd.__version__, "2.0.0")
    ):
        warn_pandas_less_than_2_0_and_sqlalchemy_greater_than_or_equal_2_0()
        with warnings.catch_warnings():
            # Note that RemovedIn20Warning is the warning class that we see from sqlalchemy
            # but using the base class here since sqlalchemy is an optional dependency and this
            # warning type only exists in sqlalchemy < 2.0.
            warnings.filterwarnings(action="ignore", category=DeprecationWarning)
            return_value = pd.read_sql_query(sql=sql, con=con, **kwargs)
    else:
        return_value = pd.read_sql_query(sql=sql, con=con, **kwargs)
    return return_value
