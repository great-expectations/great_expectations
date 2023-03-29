from __future__ import annotations

import warnings
from typing import Callable

import pandas as pd

from great_expectations.optional_imports import (
    SQLALCHEMY_NOT_IMPORTED,
    is_version_greater_or_equal,
    is_version_less_than,
    sqlalchemy,
)
from great_expectations.warnings import (
    warn_pandas_less_than_2_0_and_sqlalchemy_greater_than_or_equal_2_0,
)

try:
    from sqlalchemy.exc import RemovedIn20Warning

except ImportError:
    RemovedIn20Warning = SQLALCHEMY_NOT_IMPORTED


def execute_pandas_reader_fn(
    reader_fn: Callable, reader_options: dict
) -> pd.DataFrame | list[pd.DataFrame]:
    """Suppress warnings while executing the pandas reader functions.

    If pandas version is below 2.0 and sqlalchemy installed then suppress
    the sqlalchemy 2.0 warning and raise our own warning. pandas does not
    support sqlalchemy 2.0 until version 2.0 (see https://pandas.pydata.org/docs/dev/whatsnew/v2.0.0.html#other-enhancements)

    Args:
        reader_fn: Reader function to execute.
        reader_options: Options to pass to reader function.

    Returns:
        dataframe or list of dataframes
    """
    if is_version_less_than(pd.__version__, "2.0.0"):
        if sqlalchemy != SQLALCHEMY_NOT_IMPORTED and is_version_greater_or_equal(
            sqlalchemy.__version__, "2.0.0"
        ):
            warn_pandas_less_than_2_0_and_sqlalchemy_greater_than_or_equal_2_0()
        with warnings.catch_warnings():
            warnings.filterwarnings(action="ignore", category=RemovedIn20Warning)
            reader_fn_result: pd.DataFrame | list[pd.DataFrame] = reader_fn(
                **reader_options
            )
    else:
        reader_fn_result: pd.DataFrame | list[pd.DataFrame] = reader_fn(
            **reader_options
        )
    return reader_fn_result
