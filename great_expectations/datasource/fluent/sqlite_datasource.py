from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    List,
    Literal,
    Optional,
    Type,
    Union,
    cast,
)

import pydantic

from great_expectations.core._docs_decorators import public_api
from great_expectations.datasource.fluent.config_str import ConfigStr
from great_expectations.datasource.fluent.sql_datasource import (
    QueryAsset as SqlQueryAsset,
)
from great_expectations.datasource.fluent.sql_datasource import (
    Splitter,
    SQLDatasource,
    _SplitterOneColumnOneParam,
)
from great_expectations.datasource.fluent.sql_datasource import (
    TableAsset as SqlTableAsset,
)

if TYPE_CHECKING:
    # min version of typing_extension missing `Self`, so it can't be imported at runtime
    from typing_extensions import Self

    from great_expectations.datasource.fluent.interfaces import (
        BatchMetadata,
        BatchRequestOptions,
        DataAsset,
        SortersDefinition,
    )

# This module serves as an example of how to extend _SQLAssets for specific backends. The steps are:
# 1. Create a plain class with the extensions necessary for the specific backend.
# 2. Make 2 classes XTableAsset and XQueryAsset by mixing in the class created in step 1 with
#    sql_datasource.TableAsset and sql_datasource.QueryAsset.
#
# See SqliteDatasource, SqliteTableAsset, and SqliteQueryAsset below.


class SplitterHashedColumn(_SplitterOneColumnOneParam):
    """Split on hash value of a column.

    Args:
        hash_digits: The number of digits to truncate the hash to.
        method_name: Literal["split_on_hashed_column"]
    """

    # hash digits is the length of the hash. The md5 of the column is truncated to this length.
    hash_digits: int
    column_name: str
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


class SplitterConvertedDateTime(_SplitterOneColumnOneParam):
    """A splitter than can be used for sql engines that represents datetimes as strings.

    The SQL engine that this currently supports is SQLite since it stores its datetimes as
    strings.
    The DatetimeSplitter will also work for SQLite and may be more intuitive.
    """

    # date_format_strings syntax is documented here:
    # https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
    # It allows for arbitrary strings so can't be validated until conversion time.
    date_format_string: str
    column_name: str
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


SqliteSplitter = Union[Splitter, SplitterHashedColumn, SplitterConvertedDateTime]


class _SQLiteAssetMixin:
    @public_api
    def add_splitter_hashed_column(
        self: Self, column_name: str, hash_digits: int
    ) -> Self:
        """Associates a hashed column splitter with this sqlite data asset.
        Args:
            column_name: The column name of the date column where year and month will be parsed out.
            hash_digits: Number of digits to truncate output of hashing function (to limit length of hashed result).
        Returns:
            This sql asset so we can use this method fluently.
        """
        return self._add_splitter(  # type: ignore[attr-defined]  # This is a mixin for a _SQLAsset
            SplitterHashedColumn(
                method_name="split_on_hashed_column",
                column_name=column_name,
                hash_digits=hash_digits,
            )
        )

    @public_api
    def add_splitter_converted_datetime(
        self: Self, column_name: str, date_format_string: str
    ) -> Self:
        """Associates a converted datetime splitter with this sqlite data asset.
        Args:
            column_name: The column name of the date column where year and month will be parsed out.
            date_format_string: Format for converting string representation of datetime to actual datetime object.
        Returns:
            This sql asset so we can use this method fluently.
        """
        return self._add_splitter(  # type: ignore[attr-defined]  # This is a mixin for a _SQLAsset
            SplitterConvertedDateTime(
                method_name="split_on_converted_datetime",
                column_name=column_name,
                date_format_string=date_format_string,
            )
        )


class SqliteTableAsset(_SQLiteAssetMixin, SqlTableAsset):
    type: Literal["table"] = "table"
    splitter: Optional[SqliteSplitter] = None  # type: ignore[assignment]  # override superclass type


class SqliteQueryAsset(_SQLiteAssetMixin, SqlQueryAsset):
    type: Literal["query"] = "query"
    splitter: Optional[SqliteSplitter] = None  # type: ignore[assignment]  # override superclass type


@public_api
class SqliteDatasource(SQLDatasource):
    """Adds a sqlite datasource to the data context.

    Args:
        name: The name of this sqlite datasource.
        connection_string: The SQLAlchemy connection string used to connect to the sqlite database.
            For example: "sqlite:///path/to/file.db"
        create_temp_table: Whether to leverage temporary tables during metric computation.
        assets: An optional dictionary whose keys are TableAsset names and whose values
            are TableAsset objects.
    """

    # class var definitions
    asset_types: ClassVar[List[Type[DataAsset]]] = [SqliteTableAsset, SqliteQueryAsset]

    # Subclass instance var overrides
    # right side of the operator determines the type name
    # left side enforces the names on instance creation
    type: Literal["sqlite"] = "sqlite"  # type: ignore[assignment]
    connection_string: Union[ConfigStr, SqliteDsn]

    _TableAsset: Type[SqlTableAsset] = pydantic.PrivateAttr(SqliteTableAsset)
    _QueryAsset: Type[SqlQueryAsset] = pydantic.PrivateAttr(SqliteQueryAsset)

    @public_api
    def add_table_asset(  # noqa: PLR0913
        self,
        name: str,
        table_name: str = "",
        schema_name: Optional[str] = None,
        order_by: Optional[SortersDefinition] = None,
        batch_metadata: Optional[BatchMetadata] = None,
    ) -> SqliteTableAsset:
        return cast(
            SqliteTableAsset,
            super().add_table_asset(
                name=name,
                table_name=table_name,
                schema_name=schema_name,
                order_by=order_by,
                batch_metadata=batch_metadata,
            ),
        )

    add_table_asset.__doc__ = SQLDatasource.add_table_asset.__doc__

    @public_api
    def add_query_asset(
        self,
        name: str,
        query: str,
        order_by: Optional[SortersDefinition] = None,
        batch_metadata: Optional[BatchMetadata] = None,
    ) -> SqliteQueryAsset:
        return cast(
            SqliteQueryAsset,
            super().add_query_asset(
                name=name, query=query, order_by=order_by, batch_metadata=batch_metadata
            ),
        )

    add_query_asset.__doc__ = SQLDatasource.add_query_asset.__doc__
