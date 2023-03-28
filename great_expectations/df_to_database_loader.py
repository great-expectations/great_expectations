from __future__ import annotations

import logging
import warnings
from typing import Callable

import pandas as pd

logger = logging.getLogger(__name__)

try:
    import sqlalchemy as sa
    from sqlalchemy import Table
    from sqlalchemy.engine import reflection
    from sqlalchemy.exc import RemovedIn20Warning
    from sqlalchemy.sql import Select

except ImportError:
    logger.debug(
        "Unable to load SqlAlchemy context; install optional sqlalchemy dependency for support"
    )
    sa = None
    reflection = None
    Table = None
    Select = None
    RemovedIn20Warning = None


def add_dataframe_to_db(
    df: pd.DataFrame,
    name: str,
    con,
    schema=None,
    if_exists: str = "fail",
    index: bool = True,
    index_label: str | None = None,
    chunksize: int | None = None,
    dtype: dict | None = None,
    method: str | Callable | None = None,
) -> None:
    """Write records stored in a DataFrame to a SQL database.

    Wrapper for `to_sql()` method in Pandas. Created as part of the effort to allow GX to be compatible
    with SqlAlchemy 2, and is used to suppress warnings that arise from implicit auto-commits.

    The need for this function will eventually go away once we migrate to Pandas 1.4.0.

    Args:
        df (pd.DataFrame): DataFrame to load into the SQL Table.
        name (str): name of SQL Table.
        con (sqlalchemy engine or connection): sqlalchemy.engine or sqlite3.Connection
        schema (str | None): Specify the schema (if database flavor supports this). If None, use
            default schema. Defaults to None.
        if_exists (str | None): Can be either 'fail', 'replace', or 'append'. Defaults to `fail`.
            * fail: Raise a ValueError.
            * replace: Drop the table before inserting new values.
            * append: Insert new values to the existing table.
        index (bool): Write DataFrame index as a column. Uses `index_label` as the column
            name in the table. Defaults to True.
        index_label (str | None):
            Column label for index column(s). If None is given (default) and
            `index` is True, then the index names are used.
        chunksize (int | None):
            Specify the number of rows in each batch to be written at a time.
            By default, all rows will be written at once.
        dtype (dict | int | float | bool | None):
            Specifying the datatype for columns. If a dictionary is used, the
            keys should be the column names and the values should be the
            SQLAlchemy types or strings for the sqlite3 legacy mode. If a
            scalar is provided, it will be applied to all columns.
        method (str | Callable | None):
            Controls the SQL insertion clause used:
                * None : Uses standard SQL ``INSERT`` clause (one per row).
                * 'multi': Pass multiple values in a single ``INSERT`` clause.
                * callable with signature ``(pd_table, conn, keys, data_iter)``.
    """
    if isinstance(con, sa.engine.Engine):
        con = con.connect()
    with warnings.catch_warnings():
        warnings.filterwarnings(action="ignore", category=RemovedIn20Warning)
        df.to_sql(
            name=name,
            con=con,
            schema=schema,
            if_exists=if_exists,
            index=index,
            index_label=index_label,
            chunksize=chunksize,
            dtype=dtype,
            method=method,
        )
