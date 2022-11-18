from __future__ import annotations

import dataclasses
import itertools
from datetime import datetime
from pprint import pformat as pf
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Type, cast

import dateutil.tz
from pydantic import Field
from pydantic import dataclasses as pydantic_dc
from typing_extensions import ClassVar, Literal

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


# For our year splitter we default the range to the last 2 year.
_CURRENT_YEAR = datetime.now(dateutil.tz.tzutc()).year
_DEFAULT_YEAR_RANGE = range(_CURRENT_YEAR - 1, _CURRENT_YEAR + 1)
_DEFAULT_MONTH_RANGE = range(1, 13)


@pydantic_dc.dataclass(frozen=True)
class ColumnSplitter:
    method_name: str
    column_name: str
    # param_defaults is a Dict where the keys are the parameters of the splitter and the values are the default
    # values are the default values if a batch request using the splitter leaves the parameter unspecified.
    # template_params: List[str]
    param_defaults: Dict[str, Iterable] = Field(default_factory=dict)

    @property
    def param_names(self) -> List[str]:
        return list(self.param_defaults.keys())


class TableAsset(DataAsset):
    # Instance fields
    type: Literal["table"] = "table"
    table_name: str
    column_splitter: Optional[ColumnSplitter] = None
    name: str

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

    def validate_batch_request(self, batch_request: BatchRequest) -> None:
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

    # This asset type will support a variety of splitters
    def add_year_and_month_splitter(
        self,
        column_name: str,
        default_year_range: Iterable[int] = _DEFAULT_YEAR_RANGE,
        default_month_range: Iterable[int] = _DEFAULT_MONTH_RANGE,
    ) -> TableAsset:
        """Associates a year month splitter with this DataAsset

        Args:
            column_name: A column name of the date column where year and month will be parsed out.
            default_year_range: When this splitter is used, say in a BatchRequest, if no value for
                year is specified, we query over all years in this range.
                will query over all the years in this default range.
            default_month_range: When this splitter is used, say in a BatchRequest, if no value for
                month is specified, we query over all months in this range.

        Returns:
            This TableAsset so we can use this method fluently.
        """
        self.column_splitter = ColumnSplitter(
            method_name="split_on_year_and_month",
            column_name=column_name,
            param_defaults={"year": default_year_range, "month": default_month_range},
        )
        return self

    def fully_specified_batch_requests(self, batch_request) -> List[BatchRequest]:
        """Populates a batch requests unspecified params producing a list of batch requests

        This method does NOT validate the batch_request. If necessary call
        TableAsset.validate_batch_request before calling this method.
        """
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
                    self.column_splitter.param_defaults[option]
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


class PostgresDatasource(Datasource):
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

    def add_table_asset(self, name: str, table_name: str) -> TableAsset:
        """Adds a table asset to this datasource.

        Args:
            name: The name of this table asset.
            table_name: The table where the data resides.

        Returns:
            The TableAsset that is added to the datasource.
        """
        asset = TableAsset(name=name, table_name=table_name)
        # TODO (kilo59): custom init for `DataAsset` to accept datasource in constructor?
        # Will most DataAssets require a `Datasource` attribute?
        asset._datasource = self
        self.assets[name] = asset
        return asset

    def get_asset(self, asset_name: str) -> TableAsset:
        """Returns the TableAsset referred to by name"""
        return super().get_asset(asset_name)  # type: ignore[return-value] # value is subclass

    # When we have multiple types of DataAssets on a datasource, the batch_request argument will be a Union type.
    # To differentiate we could use single dispatch or use an if/else (note pattern matching doesn't appear until
    # python 3.10)
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
        data_asset.validate_batch_request(batch_request)

        batch_list: List[Batch] = []
        column_splitter = data_asset.column_splitter
        for request in data_asset.fully_specified_batch_requests(batch_request):
            batch_spec_kwargs = {
                "type": "table",
                "data_asset_name": data_asset.name,
                "table_name": data_asset.table_name,
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
            data, _ = self.execution_engine.get_batch_data_and_markers(
                batch_spec=SqlAlchemyDatasourceBatchSpec(**batch_spec_kwargs)
            )
            batch_list.append(
                Batch(
                    datasource=self,
                    data_asset=data_asset,
                    batch_request=request,
                    data=data,
                )
            )
        return batch_list
