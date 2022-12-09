from __future__ import annotations

import copy
import dataclasses
import itertools
from pprint import pformat as pf
from typing import TYPE_CHECKING, Dict, List, Optional, Type, Union, cast

import pydantic
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
    from great_expectations.execution_engine import ExecutionEngine


class PostgresDatasourceError(Exception):
    pass


class BatchRequestError(Exception):
    pass


@pydantic_dc.dataclass(frozen=True)
class ColumnSplitter:
    column_name: str
    method_name: str
    param_names: List[str]

    def param_defaults(self, data_asset: DataAsset) -> Dict[str, List]:
        raise NotImplementedError


@pydantic_dc.dataclass(frozen=True)
class SqlYearMonthSplitter(ColumnSplitter):
    method_name: str = "split_on_year_and_month"
    param_names: List[str] = pydantic.Field(default_factory=lambda: ["year", "month"])

    def param_defaults(self, data_asset: DataAsset) -> Dict[str, List]:
        """Query the database to get the years and months to split over.

        Args:
            data_asset: A TableAsset over which we want to split the data.
        """
        # This column splitter is only relevant to SQL data assets so we do some assertions
        # to validate this.
        from great_expectations.execution_engine import SqlAlchemyExecutionEngine

        assert isinstance(data_asset, TableAsset), "data_asset must be a TableAsset"
        assert isinstance(
            data_asset.datasource.execution_engine, SqlAlchemyExecutionEngine
        )
        with data_asset.datasource.execution_engine.engine.connect() as conn:
            # We opt to use a raw string instead of sqlalchemy.txt because we don't
            # need any of the features for this query
            # https://docs.sqlalchemy.org/en/14/core/sqlelement.html#sqlalchemy.sql.expression.text
            col = self.column_name
            q = f"select min({col}), max({col}) from {data_asset.table_name}"
            min_dt, max_dt = list(conn.execute(q))[0]
        year: List[int] = list(range(min_dt.year, max_dt.year + 1))
        month: List[int]
        if min_dt.year == max_dt.year:
            month = list(range(min_dt.month, max_dt.month + 1))
        else:
            month = list(range(1, 13))
        return {"year": year, "month": month}


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
    column_splitter: Optional[ColumnSplitter] = None
    name: str
    _order_by: List[BatchSorter] = pydantic.PrivateAttr()

    def __init__(self, **kwargs):
        # I `pop("order_by", None) or []` instead of `pop("order_by", [])` because if someone
        # passes in `order_by=None`, I want this variable to be `[]` and not `None`.
        # `pop("order_by", [])` will return None since the order_by key exists in this case.
        order_by = kwargs.pop("order_by", None) or []
        self._order_by = _batch_sorter_from_list(order_by)
        super().__init__(**kwargs)

    @property
    def order_by(self) -> List[BatchSorter]:
        return self._order_by

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
        self._order_by = _batch_sorter_from_list(sorters)
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


class PostgresDatasource(Datasource):
    """Postgres datasource

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
    type: Literal["postgres"] = "postgres"
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
        asset = TableAsset(name=name, table_name=table_name, order_by=order_by)
        # TODO (kilo59): custom init for `DataAsset` to accept datasource in constructor?
        # Will most DataAssets require a `Datasource` attribute?
        asset._datasource = self
        self.assets[name] = asset
        return asset

    def get_asset(self, asset_name: str) -> TableAsset:
        """Returns the TableAsset referred to by name"""
        return super().get_asset(asset_name)  # type: ignore[return-value] # value is subclass

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
