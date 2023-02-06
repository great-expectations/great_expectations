from __future__ import annotations

import copy
import itertools
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Dict,
    List,
    NamedTuple,
    Optional,
    Sequence,
    Type,
    Union,
    cast,
)

import pydantic
from pydantic import dataclasses as pydantic_dc
from typing_extensions import Literal

from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.experimental.datasources.interfaces import (
    Batch,
    BatchRequest,
    BatchRequestOptions,
    BatchSortersDefinition,
    DataAsset,
    Datasource,
    TestConnectionError,
)

SQLALCHEMY_IMPORTED = False
try:
    import sqlalchemy

    SQLALCHEMY_IMPORTED = True
except ImportError:
    pass

if TYPE_CHECKING:
    from great_expectations.execution_engine import ExecutionEngine


class SQLDatasourceError(Exception):
    pass


class SQLDatasourceWarning(UserWarning):
    pass


@pydantic_dc.dataclass(frozen=True)
class ColumnSplitter:
    column_name: str
    method_name: str
    param_names: Sequence[str]

    def param_defaults(self, sql_asset: _SQLAsset) -> Dict[str, List]:
        raise NotImplementedError

    @pydantic.validator("method_name")
    def _splitter_method_exists(cls, v: str):
        """Fail early if the `method_name` does not exist and would fail at runtime."""
        # NOTE (kilo59): this could be achieved by simply annotating the method_name field
        # as a `SplitterMethod` enum but we get cyclic imports.
        # This also adds the enums to the generated json schema.
        # https://docs.pydantic.dev/usage/types/#enums-and-choices
        # We could use `update_forward_refs()` but would have to change this to a BaseModel
        # https://docs.pydantic.dev/usage/postponed_annotations/
        from great_expectations.execution_engine.split_and_sample.data_splitter import (
            SplitterMethod,
        )

        method_members = set(SplitterMethod)
        if v not in method_members:
            permitted_values_str = "', '".join([m.value for m in method_members])
            raise ValueError(f"unexpected value; permitted: '{permitted_values_str}'")
        return v


class DatetimeRange(NamedTuple):
    min: datetime
    max: datetime


@pydantic_dc.dataclass(frozen=True)
class SqlYearMonthSplitter(ColumnSplitter):
    method_name: Literal["split_on_year_and_month"] = "split_on_year_and_month"
    param_names: List[Literal["year", "month"]] = pydantic.Field(
        default_factory=lambda: ["year", "month"]
    )

    def param_defaults(self, sql_asset: _SQLAsset) -> Dict[str, List]:
        """Query sql database to get the years and months to split over.

        Args:
            sql_asset: A SQL DataAsset over which we want to split the data.
        """
        return _query_for_year_and_month(
            sql_asset, self.column_name, _get_sql_datetime_range
        )


def _query_for_year_and_month(
    sql_asset: _SQLAsset,
    column_name: str,
    query_datetime_range: Callable[
        [sqlalchemy.engine.base.Connection, sqlalchemy.sql.Selectable, str],
        DatetimeRange,
    ],
) -> Dict[str, List]:
    from great_expectations.execution_engine import SqlAlchemyExecutionEngine

    execution_engine = sql_asset.datasource.get_execution_engine()
    assert isinstance(execution_engine, SqlAlchemyExecutionEngine)

    with execution_engine.engine.connect() as conn:
        datetimes: DatetimeRange = query_datetime_range(
            conn,
            sql_asset.as_selectable(),
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
    conn: sqlalchemy.engine.base.Connection,
    selectable: sqlalchemy.sql.Selectable,
    col_name: str,
) -> DatetimeRange:
    import sqlalchemy as sa

    column = sa.column(col_name)
    query = sa.select([sa.func.min(column), sa.func.max(column)]).select_from(
        selectable
    )
    min_max_dt = list(
        conn.execute(query.compile(compile_kwargs={"literal_binds": True}))
    )[0]
    if min_max_dt[0] is None or min_max_dt[1] is None:
        raise SQLDatasourceError(
            f"Data date range can not be determined for the query: {query}. The returned range was {min_max_dt}."
        )
    return DatetimeRange(min=min_max_dt[0], max=min_max_dt[1])


class _SQLAsset(DataAsset):
    # Instance fields
    type: Literal["_sqlasset"] = "_sqlasset"
    column_splitter: Optional[SqlYearMonthSplitter] = None
    name: str

    def batch_request_options_template(
        self,
    ) -> BatchRequestOptions:
        """A BatchRequestOptions template for get_batch_request.

        Returns:
            A BatchRequestOptions dictionary with the correct shape that get_batch_request
            will understand. All the option values are defaulted to None.
        """
        template: BatchRequestOptions = {}
        if not self.column_splitter:
            return template
        return {p: None for p in self.column_splitter.param_names}

    # This asset type will support a variety of splitters
    def add_year_and_month_splitter(
        self,
        column_name: str,
    ) -> _SQLAsset:
        """Associates a year month splitter with this DataAsset

        Args:
            column_name: A column name of the date column where year and month will be parsed out.

        Returns:
            This DataAsset so we can use this method fluently.
        """
        self.column_splitter = SqlYearMonthSplitter(
            column_name=column_name,
        )
        return self

    def _fully_specified_batch_requests(self, batch_request) -> List[BatchRequest]:
        """Populates a batch requests unspecified params producing a list of batch requests."""
        if self.column_splitter is None:
            # Currently batch_request.options is complete determined by the presence of a
            # column splitter. If column_splitter is None, then there are no specifiable options
            # so we return early.
            # In the future, if there are options that are not determined by the column splitter
            # this check will have to be generalized.
            return [batch_request]

        # Make a list of the specified and unspecified params in batch_request
        specified_options = []
        unspecified_options = []
        options_template = self.batch_request_options_template()
        for option_name in options_template.keys():
            if (
                option_name in batch_request.options
                and batch_request.options[option_name] is not None
            ):
                specified_options.append(option_name)
            else:
                unspecified_options.append(option_name)

        # Make a list of the all possible batch_request.options by expanding out the unspecified
        # options
        batch_requests: List[BatchRequest] = []

        if not unspecified_options:
            batch_requests.append(batch_request)
        else:
            # All options are defined by the splitter, so we look at its default values to fill
            # in the option values.
            default_option_values = []
            for option in unspecified_options:
                default_option_values.append(
                    self.column_splitter.param_defaults(self)[option]
                )
            for option_values in itertools.product(*default_option_values):
                # Add options from specified options
                options = {
                    name: batch_request.options[name] for name in specified_options
                }
                # Add options from unspecified options
                for i, option_value in enumerate(option_values):
                    options[unspecified_options[i]] = option_value
                batch_requests.append(
                    BatchRequest(
                        datasource_name=batch_request.datasource_name,
                        data_asset_name=batch_request.data_asset_name,
                        options=options,
                    )
                )
        return batch_requests

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
        self._validate_batch_request(batch_request)

        batch_list: List[Batch] = []
        column_splitter = self.column_splitter
        batch_spec_kwargs: dict[str, str | dict | None]
        for request in self._fully_specified_batch_requests(batch_request):
            batch_metadata = copy.deepcopy(request.options)
            batch_spec_kwargs = self._create_batch_spec_kwargs()
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
            # Creating the batch_spec is our hook into the execution engine.
            batch_spec = SqlAlchemyDatasourceBatchSpec(**batch_spec_kwargs)
            execution_engine: ExecutionEngine = self.datasource.get_execution_engine()
            data, markers = execution_engine.get_batch_data_and_markers(
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

            # Some pydantic annotations are postponed due to circular imports.
            # Batch.update_forward_refs() will set the annotations before we
            # instantiate the Batch class since we can import them in this scope.
            Batch.update_forward_refs()
            batch_list.append(
                Batch(
                    datasource=self.datasource,
                    data_asset=self,
                    batch_request=request,
                    data=data,
                    metadata=batch_metadata,
                    legacy_batch_markers=markers,
                    legacy_batch_spec=batch_spec,
                    legacy_batch_definition=batch_definition,
                )
            )
        self.sort_batches(batch_list)
        return batch_list

    def _create_batch_spec_kwargs(self) -> dict[str, Any]:
        """Creates batch_spec_kwargs used to instantiate a SqlAlchemyDatasourceBatchSpec

        This is called by get_batch_list_from_batch_request to generate the batches.

        Returns:
            A dictionary that will be passed to SqlAlchemyDatasourceBatchSpec(**returned_dict)
        """
        raise NotImplementedError

    def as_selectable(self) -> sqlalchemy.sql.Selectable:
        """Returns a Selectable that can be used to query this data

        Returns:
            A Selectable that can be used in a from clause to query this data
        """
        raise NotImplementedError


class QueryAsset(_SQLAsset):
    # Instance fields
    type: Literal["query"] = "query"  # type: ignore[assignment]
    query: str

    def test_connection(self) -> None:
        pass

    @pydantic.validator("query")
    def query_must_start_with_select(cls, v: str):
        query = v.lstrip()
        if not (query.upper().startswith("SELECT") and query[6].isspace()):
            raise ValueError("query must start with 'SELECT' followed by a whitespace.")
        return v

    def as_selectable(self) -> sqlalchemy.sql.Selectable:
        """Returns the Selectable that is used to retrieve the data.

        This can be used in a subselect FROM clause for queries against this data.
        """
        import sqlalchemy as sa

        return sa.select(sa.text(self.query.lstrip()[6:])).subquery()

    def _create_batch_spec_kwargs(self) -> dict[str, Any]:
        return {
            "data_asset_name": self.name,
            "query": self.query,
            "temp_table_schema_name": None,
            "batch_identifiers": {},
        }


class TableAsset(_SQLAsset):
    # Instance fields
    type: Literal["table"] = "table"  # type: ignore[assignment]
    table_name: str
    schema_name: Optional[str] = None

    def test_connection(self) -> None:
        """Test the connection for the TableAsset.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        assert isinstance(self.datasource, SQLDatasource)
        engine: sqlalchemy.engine.Engine = self.datasource.get_engine()
        inspector: sqlalchemy.engine.Inspector = sqlalchemy.inspect(engine)

        table_str = (
            f"{self.schema_name}.{self.table_name}"
            if self.schema_name
            else self.table_name
        )

        if self.schema_name and self.schema_name not in inspector.get_schema_names():
            raise TestConnectionError(
                f'Attempt to connect to table: "{table_str}" failed because the schema '
                f'"{self.schema_name}" does not exist.'
            )

        table_exists = sqlalchemy.inspect(engine).has_table(
            table_name=self.table_name,
            schema=self.schema_name,
        )
        if not table_exists:
            raise TestConnectionError(
                f'Attempt to connect to table: "{table_str}" failed because the table '
                f'"{self.table_name}" does not exist.'
            )

    def as_selectable(self) -> sqlalchemy.sql.Selectable:
        """Returns the table as a sqlalchemy Selectable.

        This can be used in a from clause for a query against this data.
        """
        import sqlalchemy as sa

        return sa.text(self.table_name)

    def _create_batch_spec_kwargs(self) -> dict[str, Any]:
        return {
            "type": "table",
            "data_asset_name": self.name,
            "table_name": self.table_name,
            "schema_name": self.schema_name,
            "batch_identifiers": {},
        }


class SQLDatasource(Datasource):
    """Adds a generic SQL datasource to the data context.

    Args:
        name: The name of this datasource.
        connection_string: The SQLAlchemy connection string used to connect to the database.
            For example: "postgresql+psycopg2://postgres:@localhost/test_database"
        assets: An optional dictionary whose keys are SQL DataAsset names and whose values
            are SQL DataAsset objects.
    """

    # class var definitions
    asset_types: ClassVar[List[Type[DataAsset]]] = [TableAsset, QueryAsset]

    # right side of the operator determines the type name
    # left side enforces the names on instance creation
    type: Literal["sql"] = "sql"
    connection_string: str
    # We need to explicitly add each asset type to the Union due to how
    # deserialization is implemented in our pydantic base model.
    assets: Dict[str, Union[TableAsset, QueryAsset]] = {}

    # private attrs
    _cached_connection_string: str = pydantic.PrivateAttr("")
    _engine: Union[sqlalchemy.engine.Engine, None] = pydantic.PrivateAttr(None)

    @property
    def execution_engine_type(self) -> Type[ExecutionEngine]:
        """Returns the default execution engine type."""
        from great_expectations.execution_engine import SqlAlchemyExecutionEngine

        return SqlAlchemyExecutionEngine

    def get_engine(self) -> sqlalchemy.engine.Engine:
        if self.connection_string != self._cached_connection_string or not self._engine:
            # validate that SQL Alchemy was successfully imported and attempt to create an engine
            if SQLALCHEMY_IMPORTED:
                try:
                    self._engine = sqlalchemy.create_engine(self.connection_string)
                except Exception as e:
                    # connection_string has passed pydantic validation, but still fails to create a sqlalchemy engine
                    # one possible case is a missing plugin (e.g. psycopg2)
                    raise SQLDatasourceError(
                        "Unable to create a SQLAlchemy engine from "
                        f"connection_string: {self.connection_string} due to the "
                        f"following exception: {str(e)}"
                    ) from e
                self._cached_connection_string = self.connection_string
            else:
                raise SQLDatasourceError(
                    "Unable to create SQLDatasource due to missing sqlalchemy dependency."
                )
        return self._engine

    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the SQLDatasource.

        Args:
            test_assets: If assets have been passed to the SQLDatasource, whether to test them as well.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        try:
            engine: sqlalchemy.engine.Engine = self.get_engine()
            engine.connect()
        except Exception as e:
            raise TestConnectionError(
                "Attempt to connect to datasource failed with the following error message: "
                f"{str(e)}"
            ) from e
        if self.assets and test_assets:
            for asset in self.assets.values():
                asset._datasource = self
                asset.test_connection()

    def add_table_asset(
        self,
        name: str,
        table_name: str,
        schema_name: Optional[str] = None,
        order_by: Optional[BatchSortersDefinition] = None,
    ) -> TableAsset:
        """Adds a table asset to this datasource.

        Args:
            name: The name of this table asset.
            table_name: The table where the data resides.
            schema_name: The schema that holds the table.
            order_by: A list of BatchSorters or BatchSorter strings.

        Returns:
            The TableAsset that is added to the datasource.
        """
        asset = TableAsset(
            name=name,
            table_name=table_name,
            schema_name=schema_name,
            order_by=order_by or [],  # type: ignore[arg-type]  # coerce list[str]
            # see DataAsset._parse_order_by_sorter()
        )
        return self.add_asset(asset)

    def add_query_asset(
        self,
        name: str,
        query: str,
        order_by: Optional[BatchSortersDefinition] = None,
    ) -> QueryAsset:
        """Adds a query asset to this datasource.

        Args:
            name: The name of this table asset.
            query: The SELECT query to selects the data to validate. It must begin with the "SELECT".
            order_by: A list of BatchSorters or BatchSorter strings.

        Returns:
            The QueryAsset that is added to the datasource.
        """
        asset = QueryAsset(
            name=name,
            query=query,
            order_by=order_by or [],  # type: ignore[arg-type]  # coerce list[str]
        )
        return self.add_asset(asset)
