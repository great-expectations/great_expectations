from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, ClassVar, Dict, List, Optional, Type, TypeVar, Union

import pydantic
from pydantic import dataclasses as pydantic_dc

from great_expectations.experimental.datasources.interfaces import DataAsset

if TYPE_CHECKING:
    import sqlalchemy

from typing_extensions import Literal

from great_expectations.experimental.datasources.sql_datasource import (
    BatchSortersDefinition,
    ColumnSplitter,
    DatetimeRange,
    QueryAsset,
    SQLDatasource,
    SQLDatasourceError,
    TableAsset,
    _query_for_year_and_month,
    _SQLAsset,
)


class _SplitterMixin:
    def add_year_and_month_splitter(
        self,
        column_name: str,
    ) -> _SQLAsset:
        """Associates a year month splitter with this sqlite data asset.

        This must be used as a mixin for a sqlite data asset.

        Args:
            column_name: A column name of the date column where year and month will be parsed out.

        Returns:
            This SqliteTableAsset so we can use this method fluently.
        """
        assert isinstance(
            self, tuple(_SqliteAssets)
        ), "_SplitterMixin can only be mixed into a sqlite data asset."
        self.column_splitter = SqliteYearMonthSplitter(
            column_name=column_name,
        )
        return self  # type: ignore[return-value]  # See isinstance check above.


class SqliteTableAsset(_SplitterMixin, TableAsset):
    # Subclass overrides
    type: Literal["sqlite_table"] = "sqlite_table"  # type: ignore[assignment]
    column_splitter: Optional[SqliteYearMonthSplitter] = None  # type: ignore[assignment]


class SqliteQueryAsset(_SplitterMixin, QueryAsset):
    # Subclass overrides
    type: Literal["sqlite_query"] = "sqlite_query"  # type: ignore[assignment]
    column_splitter: Optional[SqliteYearMonthSplitter] = None  # type: ignore[assignment]


@pydantic_dc.dataclass(frozen=True)
class SqliteYearMonthSplitter(ColumnSplitter):
    method_name: Literal["split_on_year_and_month"] = "split_on_year_and_month"
    # noinspection Pydantic
    param_names: List[Literal["year", "month"]] = pydantic.Field(
        default_factory=lambda: ["year", "month"]
    )

    def param_defaults(self, sql_asset: _SQLAsset) -> Dict[str, List]:
        """Query sqlite database to get the years and months to split over.

        Args:
            sql_asset: A Sqlite*Asset over which we want to split the data.
        """
        if not isinstance(sql_asset, tuple(_SqliteAssets)):
            raise SQLDatasourceError(
                "SQL asset passed to SqliteYearMonthSplitter is not a Sqlite*Asset. It is "
                f"{sql_asset}"
            )

        return _query_for_year_and_month(
            sql_asset, self.column_name, _get_sqlite_datetime_range
        )


def _get_sqlite_datetime_range(
    conn: sqlalchemy.engine.base.Connection,
    selectable: sqlalchemy.sql.Selectable,
    col_name: str,
) -> DatetimeRange:
    import sqlalchemy as sa

    column = sa.column(col_name)
    query = sa.select(
        [
            sa.func.strftime("%Y%m%d", sa.func.min(column)),
            sa.func.strftime("%Y%m%d", sa.func.max(column)),
        ]
    ).select_from(selectable)
    min_max_dt = [
        datetime.strptime(dt, "%Y%m%d")
        for dt in list(
            conn.execute(query.compile(compile_kwargs={"literal_binds": True}))
        )[0]
    ]
    if min_max_dt[0] is None or min_max_dt[1] is None:
        raise SQLDatasourceError(
            f"Data date range can not be determined for the query: {query}. The returned range was {min_max_dt}."
        )
    return DatetimeRange(min=min_max_dt[0], max=min_max_dt[1])


_SqliteAssets: List[Type[DataAsset]] = [SqliteTableAsset, SqliteQueryAsset]
# Unfortunately the following types can't be derived from _SqliteAssets above because mypy doesn't
# support programmatically unrolling this list, eg Union[*_SqliteAssets] is not supported.
SqliteAssetTypes = Union[SqliteTableAsset, SqliteQueryAsset]
SqliteAssetType = TypeVar("SqliteAssetType", SqliteTableAsset, SqliteQueryAsset)


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

    # class var definitions
    asset_types: ClassVar[List[Type[DataAsset]]] = _SqliteAssets

    # Subclass instance var overrides
    # right side of the operator determines the type name
    # left side enforces the names on instance creation
    type: Literal["sqlite"] = "sqlite"  # type: ignore[assignment]
    connection_string: SqliteDsn
    assets: Dict[str, SqliteAssetTypes] = {}  # type: ignore[assignment]

    def add_table_asset(
        self,
        name: str,
        table_name: str,
        schema_name: Optional[str] = None,
        order_by: Optional[BatchSortersDefinition] = None,
    ) -> SqliteTableAsset:
        """Adds a sqlite table asset to this sqlite datasource.

        Args:
            name: The name of this table asset.
            table_name: The table where the data resides.
            schema_name: The schema that holds the table.
            order_by: A list of BatchSorters or BatchSorter strings.

        Returns:
            The SqliteTableAsset that is added to the datasource.
        """
        asset = SqliteTableAsset(
            name=name,
            table_name=table_name,
            schema_name=schema_name,
            order_by=order_by or [],  # type: ignore[arg-type]  # coerce list[str]
            # see TableAsset._parse_order_by_sorter()
        )
        asset._datasource = self
        self.assets[name] = asset
        return asset

    def add_query_asset(
        self,
        name: str,
        query: str,
        order_by: Optional[BatchSortersDefinition] = None,
    ) -> SqliteQueryAsset:
        """Adds a sqlite query asset to this sqlite datasource.

        Args:
            name: The name of this table asset.
            query: The SELECT query to selects the data to validate. It must begin with the "SELECT".
            order_by: A list of BatchSorters or BatchSorter strings.

        Returns:
            The SqliteTableAsset that is added to the datasource.
        """
        asset = SqliteQueryAsset(
            name=name,
            query=query,
            order_by=order_by or [],  # type: ignore[arg-type]  # coerce list[str]
            # see TableAsset._parse_order_by_sorter()
        )
        asset._datasource = self
        self.assets[name] = asset
        return asset
