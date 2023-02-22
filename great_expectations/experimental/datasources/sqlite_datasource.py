from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, List, Type

import pydantic
from typing_extensions import Literal, Self

from great_expectations.experimental.datasources.sql_datasource import (
    ColumnSplitterConvertedDateTime,
    ColumnSplitterHashedColumn,
    SQLDatasource,
    _QueryAsset,
    _SQLAsset,
    _TableAsset,
)

if TYPE_CHECKING:
    from great_expectations.experimental.datasources.interfaces import DataAsset


class SqliteDsn(pydantic.AnyUrl):
    allowed_schemes = {
        "sqlite",
        "sqlite+pysqlite",
        "sqlite+aiosqlite",
        "sqlite+pysqlcipher",
    }
    host_required = False


class _SQLiteAsset(_SQLAsset):
    def add_splitter_hashed_column(
        self: Self, column_name: str, hash_digits: int
    ) -> Self:
        return self._add_splitter(
            ColumnSplitterHashedColumn(
                method_name="split_on_hashed_column",
                column_name=column_name,
                hash_digits=hash_digits,
            )
        )

    def add_splitter_converted_datetime(
        self: Self, column_name: str, date_format_string: str
    ) -> Self:
        return self._add_splitter(
            ColumnSplitterConvertedDateTime(
                method_name="split_on_converted_datetime",
                column_name=column_name,
                date_format_string=date_format_string,
            )
        )


class SqliteTableAsset(_TableAsset, _SQLiteAsset):
    type: Literal["table"] = "sqlite_table"


class SqliteQueryAsset(_QueryAsset, _SQLiteAsset):
    type: Literal["query"] = "sqlite_query"


class SqliteDatasource(SQLDatasource):
    """Adds a sqlite datasource to the data context.

    Args:
        name: The name of this sqlite datasource.
        connection_string: The SQLAlchemy connection string used to connect to the sqlite database.
            For example: "sqlite:///path/to/file.db"
        assets: An optional dictionary whose keys are TableAsset names and whose values
            are TableAsset objects.
    """

    # class var definitions
    asset_types: ClassVar[List[Type[DataAsset]]] = [SqliteTableAsset, SqliteQueryAsset]

    # Subclass instance var overrides
    # right side of the operator determines the type name
    # left side enforces the names on instance creation
    type: Literal["sqlite"] = "sqlite"  # type: ignore[assignment]
    connection_string: SqliteDsn

    _TableAsset: Type = pydantic.PrivateAttr(SqliteTableAsset)
    _QueryAsset: Type = pydantic.PrivateAttr(SqliteQueryAsset)
