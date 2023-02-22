from __future__ import annotations

import pydantic
from typing_extensions import Literal

from great_expectations.experimental.datasources.sql_datasource import (
    ColumnSplitterConvertedDateTime,
    ColumnSplitterHashedColumn,
    SQLDatasource,
    _SQLAsset,
)


class SqliteDsn(pydantic.AnyUrl):
    allowed_schemes = {
        "sqlite",
        "sqlite+pysqlite",
        "sqlite+aiosqlite",
        "sqlite+pysqlcipher",
    }
    host_required = False


class SqliteDatasource(SQLDatasource):
    """Adds a sqlite datasource to the data context.

    Args:
        name: The name of this sqlite datasource.
        connection_string: The SQLAlchemy connection string used to connect to the sqlite database.
            For example: "sqlite:///path/to/file.db"
        assets: An optional dictionary whose keys are TableAsset names and whose values
            are TableAsset objects.
    """

    # Subclass instance var overrides
    # right side of the operator determines the type name
    # left side enforces the names on instance creation
    type: Literal["sqlite"] = "sqlite"  # type: ignore[assignment]
    connection_string: SqliteDsn

    # These column splitters will raise an exception if not used with sqlite so I've
    # put these add splitter methods on this subclass.
    def add_splitter_hashed_column(
        self, column_name: str, hash_digits: int
    ) -> _SQLAsset:
        return self._add_splitter(
            ColumnSplitterHashedColumn(
                method_name="split_on_hashed_column",
                column_name=column_name,
                hash_digits=hash_digits,
            )
        )

    def add_splitter_converted_datetime(
        self, column_name: str, date_format_string: str
    ) -> _SQLAsset:
        return self._add_splitter(
            ColumnSplitterConvertedDateTime(
                method_name="split_on_converted_datetime",
                column_name=column_name,
                date_format_string=date_format_string,
            )
        )
