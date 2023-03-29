from __future__ import annotations

import warnings
from typing import Callable

import pandas as pd

from great_expectations.optional_imports import (
    SQLALCHEMY_NOT_IMPORTED,
    is_version_greater_or_equal,
    is_version_less_than_or_equal,
    sqlalchemy_version_check,
)
from great_expectations.warnings import (
    warn_pandas_less_than_1_4_and_sqlalchemy_greater_than_or_equal_2_0,
)

try:
    import sqlalchemy as sa
    from sqlalchemy.exc import RemovedIn20Warning

    sqlalchemy_version_check(sa.__version__)

except ImportError:
    sa = SQLALCHEMY_NOT_IMPORTED
    RemovedIn20Warning = SQLALCHEMY_NOT_IMPORTED


def execute_pandas_reader_fn(reader_fn: Callable, reader_options):
    """If pandas version is in a certain range and sqlalchemy installed then suppress
    the sqlalchemy 2.0 warning and raise our own warning."""
    if is_version_less_than_or_equal(pd.__version__, "1.5.3"):
        if sa != SQLALCHEMY_NOT_IMPORTED and is_version_greater_or_equal(
            sa.__version__, "2.0.0"
        ):
            warn_pandas_less_than_1_4_and_sqlalchemy_greater_than_or_equal_2_0()
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
