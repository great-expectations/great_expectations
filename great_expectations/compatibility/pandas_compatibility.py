from __future__ import annotations

from typing import Literal

import pandas as pd

from great_expectations.compatibility.not_imported import (
    is_version_less_than,
)


def execute_pandas_to_datetime(  # noqa: PLR0913
    arg,
    errors: Literal["raise", "coerce", "ignore"] = "raise",
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
        errors (strs): ignore, raise or coerce.
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
        if format is None:  # noqa: PLR5501
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
