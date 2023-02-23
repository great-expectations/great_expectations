from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, Optional, Type, Union

import pydantic
from typing_extensions import Literal, Self

from great_expectations.experimental.datasources.sql_datasource import (
    ColumnSplitter,
    QueryAssetP,
    SQLDatasource,
    TableAssetP,
    _ColumnSplitter,
    _QueryAsset,
    _SQLAsset,
    _TableAsset,
)

if TYPE_CHECKING:
    from great_expectations.experimental.datasources.interfaces import (
        BatchRequestOptions,
        DataAsset,
    )


class ColumnSplitterHashedColumn(_ColumnSplitter):
    # hash digits is the length of the hash. The md5 of the column is truncated to this length.
    hash_digits: int
    method_name: Literal["split_on_hashed_column"] = "split_on_hashed_column"

    @property
    def param_names(self) -> List[str]:
        return ["hash"]

    def splitter_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name, "hash_digits": self.hash_digits}

    def batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        if "hash" not in options:
            raise ValueError(
                "'hash' must be specified in the batch request options to create a batch identifier"
            )
        return {self.column_name: options["hash"]}


class ColumnSplitterConvertedDateTime(_ColumnSplitter):
    """A column splitter than can be used for sql engines that represents datetimes as strings.

    The SQL engine that this currently supports is SQLite since it stores its datetimes as
    strings.
    The DatetimeColumnSplitter will also work for SQLite and may be more intuitive.
    """

    # date_format_strings syntax is documented here:
    # https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
    # It allows for arbitrary strings so can't be validated until conversion time.
    date_format_string: str
    method_name: Literal["split_on_converted_datetime"] = "split_on_converted_datetime"

    @property
    def param_names(self) -> List[str]:
        # The datetime parameter will be a string representing a datetime in the format
        # given by self.date_format_string.
        return ["datetime"]

    def splitter_method_kwargs(self) -> Dict[str, Any]:
        return {
            "column_name": self.column_name,
            "date_format_string": self.date_format_string,
        }

    def batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        if "datetime" not in options:
            raise ValueError(
                "'datetime' must be specified in the batch request options to create a batch identifier"
            )
        return {self.column_name: options["datetime"]}


class SqliteDsn(pydantic.AnyUrl):
    allowed_schemes = {
        "sqlite",
        "sqlite+pysqlite",
        "sqlite+aiosqlite",
        "sqlite+pysqlcipher",
    }
    host_required = False


SqliteColumnSplitter = Union[
    ColumnSplitter, ColumnSplitterHashedColumn, ColumnSplitterConvertedDateTime
]


class _SQLiteAsset(_SQLAsset):

    # Instance field overrides
    column_splitter: Optional[SqliteColumnSplitter] = None  # type: ignore[assignment]  # override superclass type

    def add_splitter_hashed_column(
        self: Self, column_name: str, hash_digits: int
    ) -> Self:
        return self._add_splitter(
            ColumnSplitterHashedColumn(  # type: ignore[arg-type]  # ColumnSplitterHashedColumn implements the ColumnSplitter interface
                method_name="split_on_hashed_column",
                column_name=column_name,
                hash_digits=hash_digits,
            )
        )

    def add_splitter_converted_datetime(
        self: Self, column_name: str, date_format_string: str
    ) -> Self:
        return self._add_splitter(
            ColumnSplitterConvertedDateTime(  # type: ignore[arg-type]  # ColumnSplitterHashedColumn implements the ColumnSplitter interface
                method_name="split_on_converted_datetime",
                column_name=column_name,
                date_format_string=date_format_string,
            )
        )


class SqliteTableAsset(_TableAsset, _SQLiteAsset):
    type: Literal["sqlite_table"] = "sqlite_table"


class SqliteQueryAsset(_QueryAsset, _SQLiteAsset):
    type: Literal["sqlite_query"] = "sqlite_query"


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

    _TableAsset: Type[TableAssetP] = pydantic.PrivateAttr(SqliteTableAsset)
    _QueryAsset: Type[QueryAssetP] = pydantic.PrivateAttr(SqliteQueryAsset)
