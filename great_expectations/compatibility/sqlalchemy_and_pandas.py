from __future__ import annotations

import warnings
from typing import Callable

import pandas as pd

from great_expectations.optional_imports import (
    is_version_greater_or_equal,
    is_version_less_than,
    sqlalchemy,
)
from great_expectations.warnings import (
    warn_pandas_less_than_2_0_and_sqlalchemy_greater_than_or_equal_2_0,
)


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
        if sqlalchemy and is_version_greater_or_equal(sqlalchemy.__version__, "2.0.0"):
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


def execute_pandas_to_datetime(
    arg,
    errors: str = "raise",
    dayfirst: bool = False,
    yearfirst: bool = False,
    utc: bool | None = None,
    format: str | None = None,
    exact: bool = True,
    unit: str | None = None,
    infer_datetime_format: bool = False,
    origin="unix",
    cache: bool = True,
):
    """Wrapper method for calling Pandas `to_datetime()` for either 2.0.0 and above, or below.

    Args:
        arg :  int, float, str, datetime, list, tuple, 1-d array, Series, DataFrame/dict-like
        The object to convert to a datetime.
        errors (str): ignore, raise or coerce.
            - If 'raise', then invalid parsing will raise an exception.
            - If 'coerce', then invalid parsing will be set as `NaT`.
            - If 'ignore', then invalid parsing will return the input.
        dayfirst (bool): Prefer to parse with dayfirst? Default
        yearfirst (bool): Prefer to parse with yearfirst?
        utc (bool): Control timezone-related parsing, localization and conversion. Default False.
        format (str | None):  The strftime to parse time, e.g. :const:`"%d/%m/%Y"`. Default None.
        exact (bool): How is `format` used? If True, then we require an exact match. Default True.
        unit (str): Default unit since epoch. Default is 'ns'.
        infer_datetime_format (bool): whether to infer datetime. Deprecated in pandas 2.0.0
        origin (str): reference date. Default is `unix`.
        cache (bool):  If true, then use a cache of unique, converted dates to apply the datetime conversion. Default is True.

    Returns:
        Datetime converted output.
    """
    if is_version_less_than(pd.__version__, "2.0.0"):
        return pd.to_datetime(
            arg=arg,
            errors=errors,
            dayfirst=dayfirst,
            yearfirst=yearfirst,
            utc=utc,
            format=format,
            exact=exact,
            unit=unit,
            infer_datetime_format=infer_datetime_format,
            origin=origin,
            cache=cache,
        )
    else:
        # pandas is 2.0.0 or greater
        if format is None:
            format = "mixed"
            # format = `mixed` or `ISO8601` cannot be used in combination with `exact` parameter.
            # infer_datetime_format is deprecated as of 2.0.0
            return pd.to_datetime(
                arg=arg,
                errors=errors,
                dayfirst=dayfirst,
                yearfirst=yearfirst,
                utc=utc,
                format=format,
                unit=unit,
                origin=origin,
                cache=cache,
            )
        else:
            return pd.to_datetime(
                arg=arg,
                errors=errors,
                dayfirst=dayfirst,
                yearfirst=yearfirst,
                utc=utc,
                format=format,
                exact=exact,
                unit=unit,
                origin=origin,
                cache=cache,
            )
