from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Type, cast

import pydantic
from pydantic import Field
from pydantic import dataclasses as pydantic_dc
from typing_extensions import ClassVar, Literal

from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.experimental.datasources.interfaces import (
    Batch,
    BatchRequest,
    BatchSorter,
    BatchSortersDefinition,
    ColumnSplitter,
    DataAsset,
    Datasource,
    DatetimeRange,
)

if TYPE_CHECKING:
    import sqlalchemy

    from great_expectations.execution_engine import ExecutionEngine


class SQLDatasourceError(Exception):
    pass


@pydantic_dc.dataclass(frozen=True)
class SqlYearMonthSplitter(ColumnSplitter):
    method_name: Literal["split_on_year_and_month"] = "split_on_year_and_month"
    param_names: List[Literal["year", "month"]] = pydantic.Field(
        default_factory=lambda: ["year", "month"]
    )

    def param_defaults(self, table_asset: TableAsset) -> Dict[str, List]:
        """Query sql database to get the years and months to split over.

        Args:
            table_asset: A TableAsset over which we want to split the data.
        """
        return _query_for_year_and_month(
            table_asset, self.column_name, _get_sql_datetime_range
        )


def _query_for_year_and_month(
    table_asset: TableAsset,
    column_name: str,
    query_datetime_range: Callable[
        [sqlalchemy.engine.base.Connection, str, str], DatetimeRange
    ],
) -> Dict[str, List]:
    # We should make an assertion about the execution_engine earlier. Right now it is assigned to
    # after construction. We may be able to use a hook in a property setter.
    from great_expectations.execution_engine import SqlAlchemyExecutionEngine

    assert isinstance(
        table_asset.datasource.execution_engine, SqlAlchemyExecutionEngine
    )

    with table_asset.datasource.execution_engine.engine.connect() as conn:
        datetimes: DatetimeRange = query_datetime_range(
            conn,
            table_asset.table_name,
            column_name,
        )
    year: List[int] = list(range(datetimes.min.year, datetimes.max.year + 1))
    month: List[int]
    if datetimes.min.year == datetimes.max.year:
        month = list(range(datetimes.min.month, datetimes.max.month + 1))
    else:
        month = list(range(1, 13))
    return {"year": year, "month": month}


def _get_sql_datetime_range(
    conn: sqlalchemy.engine.base.Connection, table_name: str, col_name: str
) -> DatetimeRange:
    q = f"select min({col_name}), max({col_name}) from {table_name}"
    min_max_dt = list(conn.execute(q))[0]
    return DatetimeRange(min=min_max_dt[0], max=min_max_dt[1])


class TableAsset(DataAsset):
    # Overridden inherited instance fields
    type: Literal["table"] = "table"
    column_splitter: Optional[SqlYearMonthSplitter] = None
    order_by: List[BatchSorter] = Field(default_factory=list)
    # Instance fields
    table_name: str

    def add_year_and_month_splitter(
        self,
        column_name: str,
    ) -> TableAsset:
        """Associates a year month splitter with this DataAsset

        Args:
            column_name: A column name of the date column where year and month will be parsed out.

        Returns:
            This TableAsset so we can use this method fluently.
        """
        self.column_splitter = SqlYearMonthSplitter(
            column_name=column_name,
        )
        return self

    def _batch_from_fully_specified_batch_request(self, request: BatchRequest) -> Batch:
        """Returns a Batch object from a fully specified batch request

        Args:
            request: A fully specified batch request. A fully specified batch request is
              one where all the arguments are explicitly set so corresponds to exactly 1 batch.

        Returns:
            A single Batch object specified by the batch request
        """
        batch_metadata = copy.deepcopy(request.options)
        column_splitter = self.column_splitter
        batch_spec_kwargs = {
            "type": "table",
            "data_asset_name": self.name,
            "table_name": self.table_name,
            "batch_identifiers": {},
        }
        if column_splitter:
            batch_spec_kwargs["splitter_method"] = column_splitter.method_name
            batch_spec_kwargs["splitter_kwargs"] = {
                "column_name": column_splitter.column_name
            }
            # mypy infers that batch_spec_kwargs["batch_identifiers"] is a collection, but
            # it is hardcoded to a dict above, so we cast it here.
            cast(Dict, batch_spec_kwargs["batch_identifiers"]).update(
                {column_splitter.column_name: request.options}
            )
        batch_spec = SqlAlchemyDatasourceBatchSpec(**batch_spec_kwargs)
        data, markers = self.datasource.execution_engine.get_batch_data_and_markers(
            batch_spec=batch_spec
        )

        # batch_definition (along with batch_spec and markers) is only here to satisfy a
        # legacy constraint when computing usage statistics in a validator. We hope to remove
        # it in the future.
        # imports are done inline to prevent a circular dependency with core/batch.py
        from great_expectations.core import IDDict
        from great_expectations.core.batch import BatchDefinition

        batch_definition = BatchDefinition(
            datasource_name=self.datasource.name,
            data_connector_name="experimental",
            data_asset_name=self.name,
            batch_identifiers=IDDict(batch_spec["batch_identifiers"]),
            batch_spec_passthrough=None,
        )
        return Batch(
            datasource=self.datasource,
            data_asset=self,
            batch_request=request,
            data=data,
            metadata=batch_metadata,
            legacy_batch_markers=markers,
            legacy_batch_spec=batch_spec,
            legacy_batch_definition=batch_definition,
        )


class SQLDatasource(Datasource):
    """Adds a generic SQL datasource to the data context.

    Args:
        name: The name of this datasource
        connection_str: The SQLAlchemy connection string used to connect to the database.
            For example: "postgresql+psycopg2://postgres:@localhost/test_database"
        assets: An optional dictionary whose keys are table asset names and whose values
            are TableAsset objects.
    """

    # class var definitions
    asset_types: ClassVar[List[Type[DataAsset]]] = [TableAsset]

    # right side of the operator determines the type name
    # left side enforces the names on instance creation
    type: Literal["sql"] = "sql"
    connection_string: str
    assets: Dict[str, TableAsset] = {}

    def execution_engine_type(self) -> Type[ExecutionEngine]:
        """Returns the default execution engine type."""
        from great_expectations.execution_engine import SqlAlchemyExecutionEngine

        return SqlAlchemyExecutionEngine

    def add_table_asset(
        self,
        name: str,
        table_name: str,
        order_by: Optional[BatchSortersDefinition] = None,
    ) -> TableAsset:
        """Adds a table asset to this datasource.

        Args:
            name: The name of this table asset.
            table_name: The table where the data resides.
            order_by: A list of BatchSorters or BatchSorter strings.

        Returns:
            The TableAsset that is added to the datasource.
        """
        asset = TableAsset(
            name=name,
            table_name=table_name,
            order_by=order_by or [],  # type: ignore[arg-type]  # coerce list[str]
            # see DataAsset._parse_order_by_sorter()
        )
        # TODO (kilo59): custom init for `DataAsset` to accept datasource in constructor?
        # Will most DataAssets require a `Datasource` attribute?
        asset._datasource = self
        self.assets[name] = asset
        return asset

    def get_batch_list_from_batch_request(
        self, batch_request: BatchRequest
    ) -> List[Batch]:
        """A list of batches that match the BatchRequest.

        Args:
            batch_request: A batch request for this asset. Usually obtained by calling
                get_batch_request on the asset.

        Returns:
            A list of batches that match the options specified in the batch request.
        """
        # We translate the batch_request into a BatchSpec to hook into GX core.
        data_asset = self.get_asset(batch_request.data_asset_name)
        return data_asset.get_batch_list_from_batch_request(batch_request)
