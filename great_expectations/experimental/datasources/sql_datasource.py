from __future__ import annotations

import copy
import dataclasses
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    List,
    Optional,
    Type,
    Union,
    cast,
)

import pydantic
from typing_extensions import Literal

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.experimental.datasources.experimental_base_model import (
    ExperimentalBaseModel,
)
from great_expectations.experimental.datasources.interfaces import (
    Batch,
    BatchRequest,
    BatchRequestOptions,
    BatchSorter,
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
    from great_expectations.execution_engine import SqlAlchemyExecutionEngine


class SQLDatasourceError(Exception):
    pass


class SQLDatasourceWarning(UserWarning):
    pass


class _ColumnSplitter(ExperimentalBaseModel):
    column_name: str
    method_name: str

    @property
    def param_names(self) -> List[str]:
        """Returns the parameter names that specify a batch derived from this column splitter

        For example, for ColumnSplitterYearMonth this returns ["year", "month"]. For more
        examples, please see _ColumnSplitter subclasses.
        """
        raise NotImplementedError

    def splitter_method_kwargs(self) -> Dict[str, Any]:
        """A shim to our sqlalchemy execution engine splitter methods

        We translate any internal _ColumnSplitter state and what is passed in from
        a batch_request to the splitter_kwargs required by our execution engine
        data splitters defined in:
        great_expectations.execution_engine.split_and_sample.sqlalchemy_data_splitter

        Look at _ColumnSplitter subclasses for concrete examples.
        """
        raise NotImplementedError

    def batch_batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        """Translate the batch request options to the dictionary needed to by our execution engine

        Arguments:
            options: A BatchRequest.options dictionary that specified all the fields necessary
                     to specify a batch with respect to this column splitter.

        Returns:
            A dictionary that can be added to batch_spec_kwargs["batch_identifiers"] when we
            make a SqlAlchemyDatasourceBatchSpec and run execution_engine.get_batch_data_and_markers
            for a particular batch specified by options.
        """
        raise NotImplementedError

    def param_defaults(self, sql_asset: _SQLAsset) -> List[Dict]:
        from great_expectations.execution_engine.split_and_sample.sqlalchemy_data_splitter import (
            SqlAlchemyDataSplitter,
        )

        execution_engine = sql_asset.datasource.get_execution_engine()
        splitter = SqlAlchemyDataSplitter(execution_engine.dialect_name)
        batch_identifier_data = splitter.get_data_for_batch_identifiers(
            execution_engine=execution_engine,
            selectable=sql_asset.as_selectable(),
            splitter_method_name=self.method_name,
            splitter_kwargs=self.splitter_method_kwargs(),
        )
        params: List[Dict] = []
        for identifier_data in batch_identifier_data:
            # batch_identifiers is a list of dicts that is keyed by the column name. The value is either:
            #  * A dict whose keys are batch request parameter names and values are the batch values.
            #  * A single value for the 1 batch request parameter.
            value = identifier_data[self.column_name]
            if isinstance(value, dict):
                params.append(value)
            elif len(self.param_names) == 1:
                params.append({self.param_names[0]: value})
            else:
                raise SQLDatasourceError(
                    f"identifier_data is in an unexpected form for {self.__class__} "
                    f"on column {self.column_name}: {identifier_data}"
                )
        return params

    def test_connection(self, table_asset: TableAsset) -> None:
        """Test the connection for the ColumnSplitter.

        Args:
            table_asset: The TableAsset to which the ColumnSplitter will be applied.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        datasource: SQLDatasource = table_asset.datasource
        engine: sqlalchemy.engine.Engine = datasource.get_engine()
        inspector: sqlalchemy.engine.Inspector = sqlalchemy.inspect(engine)

        columns: list[dict[str, Any]] = inspector.get_columns(
            table_name=table_asset.table_name, schema=table_asset.schema_name
        )
        column_names: list[str] = [column["name"] for column in columns]
        if self.column_name not in column_names:
            raise TestConnectionError(
                f'The column "{self.column_name}" was not found in table "{table_asset.qualified_name}"'
            )


class ColumnSplitterYear(_ColumnSplitter):
    method_name: Literal["split_on_year"] = "split_on_year"

    @property
    def param_names(self) -> List[str]:
        return ["year"]

    def splitter_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name}

    def batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        return _request_options_to_datetime_batch_spec_kwargs(self, options)


class ColumnSplitterYearAndMonth(_ColumnSplitter):
    method_name: Literal["split_on_year_and_month"] = "split_on_year_and_month"

    @property
    def param_names(self) -> List[str]:
        return ["year", "month"]

    def splitter_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name}

    def batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        return _request_options_to_datetime_batch_spec_kwargs(self, options)


class ColumnSplitterYearAndMonthAndDay(_ColumnSplitter):
    method_name: Literal[
        "split_on_year_and_month_and_day"
    ] = "split_on_year_and_month_and_day"

    @property
    def param_names(self) -> List[str]:
        return ["year", "month", "day"]

    def splitter_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name}

    def batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        return _request_options_to_datetime_batch_spec_kwargs(self, options)


class ColumnSplitterDatetimePart(_ColumnSplitter):
    datetime_parts: List[str]
    column_name: str
    method_name: Literal["split_on_date_parts"] = "split_on_date_parts"

    @property
    def param_names(self) -> List[str]:
        return self.datetime_parts

    def splitter_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name, "date_parts": self.param_names}

    def batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        return _request_options_to_datetime_batch_spec_kwargs(self, options)

    @pydantic.validator("datetime_parts", each_item=True)
    def _check_param_name_allowed(cls, v: str):
        from great_expectations.execution_engine.split_and_sample.data_splitter import (
            DatePart,
        )

        allowed_date_parts = [part.value for part in DatePart]
        assert (
            v in allowed_date_parts
        ), f"Only the following param_names are allowed: {allowed_date_parts}"
        return v


DatetimeColumnSplitter = Union[
    ColumnSplitterYear,
    ColumnSplitterYearAndMonth,
    ColumnSplitterYearAndMonthAndDay,
    ColumnSplitterDatetimePart,
]


def _request_options_to_datetime_batch_spec_kwargs(
    splitter: DatetimeColumnSplitter, options: BatchRequestOptions
) -> Dict[str, Any]:
    identifiers: Dict = {}
    for part in splitter.param_names:
        if part not in options:
            raise ValueError(
                f"'{part}' must be specified in the batch request options to create a batch identifier"
            )
        identifiers[part] = options[part]
    return {splitter.column_name: identifiers}


class ColumnSplitterColumnValue(_ColumnSplitter):
    method_name: Literal["split_on_column_value"] = "split_on_column_value"

    @property
    def param_names(self) -> List[str]:
        return [self.column_name]

    def splitter_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name}

    def batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        if self.column_name not in options:
            raise ValueError(
                f"'{self.column_name}' must be specified in the batch request options to create a batch identifier"
            )
        return {self.column_name: options[self.column_name]}


class ColumnSplitterDividedInteger(_ColumnSplitter):
    divisor: int
    method_name: Literal["split_on_divided_integer"] = "split_on_divided_integer"

    @property
    def param_names(self) -> List[str]:
        return ["quotient"]

    def splitter_method_kwargs(self) -> Dict[str, Any]:
        return {"column_name": self.column_name, "divisor": self.divisor}

    def batch_request_options_to_batch_spec_kwarg_identifiers(
        self, options: BatchRequestOptions
    ) -> Dict[str, Any]:
        if "quotient" not in options:
            raise ValueError(
                "'quotient' must be specified in the batch request options to create a batch identifier"
            )
        return {self.column_name: options["quotient"]}


ColumnSplitter = Union[
    ColumnSplitterColumnValue,
    ColumnSplitterDividedInteger,
    ColumnSplitterDatetimePart,
    ColumnSplitterYearAndMonthAndDay,
    ColumnSplitterYearAndMonth,
    ColumnSplitterYear,
]


class _SQLAsset(DataAsset):
    # Instance fields
    type: str = pydantic.Field("_sql_asset")
    column_splitter: Optional[ColumnSplitter] = None
    name: str

    # Internal attributes
    _datasource: SQLDatasource = pydantic.PrivateAttr()

    def batch_request_options_template(
        self,
    ) -> BatchRequestOptions:
        """A BatchRequestOptions template for build_batch_request.

        Returns:
            A BatchRequestOptions dictionary with the correct shape that build_batch_request
            will understand. All the option values are defaulted to None.
        """
        template: BatchRequestOptions = {}
        if not self.column_splitter:
            return template
        return {p: None for p in self.column_splitter.param_names}

    def _add_splitter(self, column_splitter: ColumnSplitter):
        self.column_splitter = column_splitter
        self.test_column_splitter_connection()
        return self

    def add_splitter_year(
        self,
        column_name: str,
    ) -> _SQLAsset:
        """Associates a year splitter with this sql data asset.
        Args:
            column_name: A column name of the date column where year and month will be parsed out.
        Returns:
            This sql asset so we can use this method fluently.
        """
        return self._add_splitter(
            ColumnSplitterYear(method_name="split_on_year", column_name=column_name)
        )

    def add_splitter_year_and_month(
        self,
        column_name: str,
    ) -> _SQLAsset:
        """Associates a year, month splitter with this sql asset.
        Args:
            column_name: A column name of the date column where year and month will be parsed out.
        Returns:
            This sql asset so we can use this method fluently.
        """
        return self._add_splitter(
            ColumnSplitterYearAndMonth(
                method_name="split_on_year_and_month", column_name=column_name
            )
        )

    def add_splitter_year_and_month_and_day(
        self,
        column_name: str,
    ) -> _SQLAsset:
        """Associates a year, month, day splitter with this sql asset.
        Args:
            column_name: A column name of the date column where year and month will be parsed out.
        Returns:
            This sql asset so we can use this method fluently.
        """
        return self._add_splitter(
            ColumnSplitterYearAndMonthAndDay(
                method_name="split_on_year_and_month_and_day", column_name=column_name
            )
        )

    def add_splitter_datetime_part(
        self, column_name: str, datetime_parts: List[str]
    ) -> _SQLAsset:
        return self._add_splitter(
            ColumnSplitterDatetimePart(
                method_name="split_on_date_parts",
                column_name=column_name,
                datetime_parts=datetime_parts,
            )
        )

    def add_splitter_column_value(self, column_name: str) -> _SQLAsset:
        return self._add_splitter(
            ColumnSplitterColumnValue(
                method_name="split_on_column_value",
                column_name=column_name,
            )
        )

    def add_splitter_divided_integer(self, column_name: str, divisor: int) -> _SQLAsset:
        return self._add_splitter(
            ColumnSplitterDividedInteger(
                method_name="split_on_divided_integer",
                column_name=column_name,
                divisor=divisor,
            )
        )

    def test_connection(self) -> None:
        pass

    def test_column_splitter_connection(self) -> None:
        pass

    @staticmethod
    def _does_matches_request_options(
        candidate: Dict, requested_options: BatchRequestOptions
    ) -> bool:
        for k, v in requested_options.items():
            if v is not None and candidate[k] != v:
                return False
        return True

    def _fully_specified_batch_requests(
        self, batch_request: BatchRequest
    ) -> List[BatchRequest]:
        """Populates a batch requests unspecified params producing a list of batch requests."""
        if self.column_splitter is None:
            # Currently batch_request.options is complete determined by the presence of a
            # column splitter. If column_splitter is None, then there are no specifiable options
            # so we return early. Since the passed in batch_request is verified, it must be the
            # empty, ie {}.
            # In the future, if there are options that are not determined by the column splitter
            # this check will have to be generalized.
            return [batch_request]

        batch_requests: List[BatchRequest] = []
        # We iterate through all possible batches as determined by the column splitter
        for params in self.column_splitter.param_defaults(self):
            # If the params from the column splitter don't match the batch request options
            # we don't create this batch.
            if not _SQLAsset._does_matches_request_options(
                params, batch_request.options
            ):
                continue
            options = copy.deepcopy(batch_request.options)
            options.update(params)
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
                build_batch_request on the asset.

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
                batch_spec_kwargs[
                    "splitter_kwargs"
                ] = column_splitter.splitter_method_kwargs()
                # mypy infers that batch_spec_kwargs["batch_identifiers"] is a collection, but
                # it is hardcoded to a dict above, so we cast it here.
                cast(Dict, batch_spec_kwargs["batch_identifiers"]).update(
                    column_splitter.batch_request_options_to_batch_spec_kwarg_identifiers(
                        request.options
                    )
                )
            # Creating the batch_spec is our hook into the execution engine.
            batch_spec = SqlAlchemyDatasourceBatchSpec(**batch_spec_kwargs)
            execution_engine: SqlAlchemyExecutionEngine = (
                self.datasource.get_execution_engine()
            )
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

    def build_batch_request(
        self, options: Optional[BatchRequestOptions] = None
    ) -> BatchRequest:
        """A batch request that can be used to obtain batches for this DataAsset.

        Args:
            options: A dict that can be used to limit the number of batches returned from the asset.
                The dict structure depends on the asset type. A template of the dict can be obtained by
                calling batch_request_options_template.

        Returns:
            A BatchRequest object that can be used to obtain a batch list from a Datasource by calling the
            get_batch_list_from_batch_request method.
        """
        if options is not None and not self._valid_batch_request_options(options):
            allowed_keys = set(self.batch_request_options_template().keys())
            actual_keys = set(options.keys())
            raise gx_exceptions.InvalidBatchRequestError(
                "Batch request options should only contain keys from the following set:\n"
                f"{allowed_keys}\nbut your specified keys contain\n"
                f"{actual_keys.difference(allowed_keys)}\nwhich is not valid.\n"
            )
        return BatchRequest(
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            options=options or {},
        )

    def _validate_batch_request(self, batch_request: BatchRequest) -> None:
        """Validates the batch_request has the correct form.

        Args:
            batch_request: A batch request object to be validated.
        """
        if not (
            batch_request.datasource_name == self.datasource.name
            and batch_request.data_asset_name == self.name
            and self._valid_batch_request_options(batch_request.options)
        ):
            expect_batch_request_form = BatchRequest(
                datasource_name=self.datasource.name,
                data_asset_name=self.name,
                options=self.batch_request_options_template(),
            )
            raise gx_exceptions.InvalidBatchRequestError(
                "BatchRequest should have form:\n"
                f"{pf(dataclasses.asdict(expect_batch_request_form))}\n"
                f"but actually has form:\n{pf(dataclasses.asdict(batch_request))}\n"
            )

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
    type: Literal["query"] = "query"
    query: str

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
    type: Literal["table"] = "table"
    table_name: str
    schema_name: Optional[str] = None

    @property
    def qualified_name(self) -> str:
        return (
            f"{self.schema_name}.{self.table_name}"
            if self.schema_name
            else self.table_name
        )

    def test_connection(self) -> None:
        """Test the connection for the TableAsset.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        datasource: SQLDatasource = self.datasource
        engine: sqlalchemy.engine.Engine = datasource.get_engine()
        inspector: sqlalchemy.engine.Inspector = sqlalchemy.inspect(engine)

        if self.schema_name and self.schema_name not in inspector.get_schema_names():
            raise TestConnectionError(
                f'Attempt to connect to table: "{self.qualified_name}" failed because the schema '
                f'"{self.schema_name}" does not exist.'
            )

        table_exists = sqlalchemy.inspect(engine).has_table(
            table_name=self.table_name,
            schema=self.schema_name,
        )
        if not table_exists:
            raise TestConnectionError(
                f'Attempt to connect to table: "{self.qualified_name}" failed because the table '
                f'"{self.table_name}" does not exist.'
            )

    def test_column_splitter_connection(self) -> None:
        if self.column_splitter:
            self.column_splitter.test_connection(table_asset=self)

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
    def execution_engine_type(self) -> Type[SqlAlchemyExecutionEngine]:
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
        order_by_sorters: list[BatchSorter] = self.parse_order_by_sorters(
            order_by=order_by
        )
        asset = TableAsset(
            name=name,
            table_name=table_name,
            schema_name=schema_name,
            order_by=order_by_sorters,
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
        order_by_sorters: list[BatchSorter] = self.parse_order_by_sorters(
            order_by=order_by
        )
        asset = QueryAsset(
            name=name,
            query=query,
            order_by=order_by_sorters,
        )
        return self.add_asset(asset)
