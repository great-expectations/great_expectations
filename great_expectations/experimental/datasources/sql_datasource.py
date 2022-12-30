from __future__ import annotations

import copy
import dataclasses
import itertools
from datetime import datetime
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Callable,
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
from pydantic import Field
from pydantic import dataclasses as pydantic_dc
from typing_extensions import ClassVar, Literal, TypeAlias

from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.experimental.datasources.interfaces import (
    Batch,
    BatchRequest,
    BatchRequestOptions,
    DataAsset,
    Datasource,
)

if TYPE_CHECKING:
    import sqlalchemy

    from great_expectations.execution_engine import ExecutionEngine


class SQLDatasourceError(Exception):
    pass


class BatchRequestError(Exception):
    pass


@pydantic_dc.dataclass(frozen=True)
class ColumnSplitter:
    column_name: str
    method_name: str
    param_names: Sequence[str]

    def param_defaults(self, table_asset: TableAsset) -> Dict[str, List]:
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


@pydantic_dc.dataclass(frozen=True)
class BatchSorter:
    metadata_key: str
    reverse: bool = False


BatchSortersDefinition: TypeAlias = Union[List[BatchSorter], List[str]]


def _batch_sorter_from_list(sorters: BatchSortersDefinition) -> List[BatchSorter]:
    if len(sorters) == 0 or isinstance(sorters[0], BatchSorter):
        # mypy gets confused here. Since BatchSortersDefinition has all elements of the
        # same type in the list so if the first on is BatchSorter so are the others.
        return cast(List[BatchSorter], sorters)
    # Likewise, sorters must be List[str] here.
    return [_batch_sorter_from_str(sorter) for sorter in cast(List[str], sorters)]


def _batch_sorter_from_str(sort_key: str) -> BatchSorter:
    """Convert a list of strings to BatchSorters

    Args:
        sort_key: A batch metadata key which will be used to sort batches on a data asset.
                  This can be prefixed with a + or - to indicate increasing or decreasing
                  sorting. If not specified, defaults to increasing order.
    """
    if sort_key[0] == "-":
        return BatchSorter(metadata_key=sort_key[1:], reverse=True)
    elif sort_key[0] == "+":
        return BatchSorter(metadata_key=sort_key[1:], reverse=False)
    else:
        return BatchSorter(metadata_key=sort_key, reverse=False)


class TableAsset(DataAsset):
    # Instance fields
    type: Literal["table"] = "table"
    table_name: str
    column_splitter: Optional[SqlYearMonthSplitter] = None
    name: str
    order_by: List[BatchSorter] = Field(default_factory=list)

    @pydantic.validator("order_by", pre=True, each_item=True)
    @classmethod
    def _parse_order_by_sorter(
        cls, v: Union[str, BatchSorter]
    ) -> Union[BatchSorter, dict]:
        if isinstance(v, str):
            if not v:
                raise ValueError("empty string")
            return _batch_sorter_from_str(v)
        return v

    def get_batch_request(
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
            raise BatchRequestError(
                "Batch request options should have a subset of keys:\n"
                f"{list(self.batch_request_options_template().keys())}\n"
                f"but actually has the form:\n{pf(options)}\n"
            )
        return BatchRequest(
            datasource_name=self._datasource.name,
            data_asset_name=self.name,
            options=options or {},
        )

    def _valid_batch_request_options(self, options: BatchRequestOptions) -> bool:
        return set(options.keys()).issubset(
            set(self.batch_request_options_template().keys())
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
            raise BatchRequestError(
                "BatchRequest should have form:\n"
                f"{pf(dataclasses.asdict(expect_batch_request_form))}\n"
                f"but actually has form:\n{pf(dataclasses.asdict(batch_request))}\n"
            )

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

    def add_sorters(self, sorters: BatchSortersDefinition) -> TableAsset:
        # NOTE: (kilo59) we could use pydantic `validate_assignment` for this
        # https://docs.pydantic.dev/usage/model_config/#options
        self.order_by = _batch_sorter_from_list(sorters)
        return self

    # This asset type will support a variety of splitters
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

    def _sort_batches(self, batch_list: List[Batch]) -> None:
        """Sorts batch_list in place.

        Args:
            batch_list: The list of batches to sort in place.
        """
        for sorter in reversed(self.order_by):
            try:
                batch_list.sort(
                    key=lambda b: b.metadata[sorter.metadata_key],
                    reverse=sorter.reverse,
                )
            except KeyError as e:
                raise KeyError(
                    f"Trying to sort {self.name} table asset batches on key {sorter.metadata_key} "
                    "which isn't available on all batches."
                ) from e

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
        self._validate_batch_request(batch_request)

        batch_list: List[Batch] = []
        column_splitter = self.column_splitter
        for request in self._fully_specified_batch_requests(batch_request):
            batch_metadata = copy.deepcopy(request.options)
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
        self._sort_batches(batch_list)
        return batch_list


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
            # see TableAsset._parse_order_by_sorter()
        )
        # TODO (kilo59): custom init for `DataAsset` to accept datasource in constructor?
        # Will most DataAssets require a `Datasource` attribute?
        asset._datasource = self
        self.assets[name] = asset
        return asset

    def get_asset(self, asset_name: str) -> TableAsset:
        """Returns the TableAsset referred to by name"""
        return super().get_asset(asset_name)

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
