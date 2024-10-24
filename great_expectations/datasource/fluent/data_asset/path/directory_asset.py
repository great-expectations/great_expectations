from __future__ import annotations

import pathlib
from abc import ABC
from functools import singledispatchmethod
from typing import TYPE_CHECKING, Generic, Optional

from great_expectations import exceptions as gx_exceptions
from great_expectations._docs_decorators import public_api
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.batch import LegacyBatchDefinition
from great_expectations.core.partitioners import (
    ColumnPartitioner,
    ColumnPartitionerDaily,
    ColumnPartitionerMonthly,
    ColumnPartitionerYearly,
)
from great_expectations.datasource.fluent import BatchRequest
from great_expectations.datasource.fluent.batch_identifier_util import make_batch_identifier
from great_expectations.datasource.fluent.constants import _DATA_CONNECTOR_NAME
from great_expectations.datasource.fluent.data_asset.path.dataframe_partitioners import (
    DataframePartitioner,
    DataframePartitionerDaily,
    DataframePartitionerMonthly,
    DataframePartitionerYearly,
)
from great_expectations.datasource.fluent.data_asset.path.path_data_asset import (
    PathDataAsset,
)
from great_expectations.datasource.fluent.data_connector import FILE_PATH_BATCH_SPEC_KEY
from great_expectations.datasource.fluent.interfaces import DatasourceT, PartitionerSortingProtocol

if TYPE_CHECKING:
    from great_expectations.alias_types import PathStr
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.datasource.fluent import BatchParameters
    from great_expectations.datasource.fluent.data_connector.batch_filter import BatchSlice


@public_api
class DirectoryDataAsset(PathDataAsset[DatasourceT, ColumnPartitioner], Generic[DatasourceT], ABC):
    """Base class for PathDataAssets which batch by combining the contents of a directory."""

    data_directory: pathlib.Path

    @public_api
    def add_batch_definition_daily(self, name: str, column: str) -> BatchDefinition:
        # todo: test column
        return self.add_batch_definition(
            name=name,
            partitioner=ColumnPartitionerDaily(
                method_name="partition_on_year_and_month_and_day", column_name=column
            ),
        )

    @public_api
    def add_batch_definition_monthly(self, name: str, column: str) -> BatchDefinition:
        # todo: test column
        return self.add_batch_definition(
            name=name,
            partitioner=ColumnPartitionerMonthly(
                method_name="partition_on_year_and_month", column_name=column
            ),
        )

    @public_api
    def add_batch_definition_yearly(self, name: str, column: str) -> BatchDefinition:
        # todo: test column
        return self.add_batch_definition(
            name=name,
            partitioner=ColumnPartitionerYearly(
                method_name="partition_on_year", column_name=column
            ),
        )

    @public_api
    def add_batch_definition_whole_directory(self, name: str) -> BatchDefinition:
        """Add a BatchDefinition which creates a single batch for the entire directory."""
        return self.add_batch_definition(name=name, partitioner=None)

    @override
    def _get_batch_definition_list(
        self, batch_request: BatchRequest
    ) -> list[LegacyBatchDefinition]:
        """Generate a batch definition list from a given batch request.

        Args:
            batch_request: Batch request used to generate batch definitions.

        Returns:
            List of a single batch definition.
        """
        if batch_request.partitioner:
            # Currently non-sql asset partitioners do not introspect the datasource for available
            # batches and only return a single batch based on specified batch_identifiers.
            batch_identifiers = batch_request.options
            if not batch_identifiers.get("path"):
                batch_identifiers["path"] = self.data_directory

            batch_definition = LegacyBatchDefinition(
                datasource_name=self._data_connector.datasource_name,
                data_connector_name=_DATA_CONNECTOR_NAME,
                data_asset_name=self._data_connector.data_asset_name,
                batch_identifiers=make_batch_identifier(batch_identifiers),
            )
            batch_definition_list = [batch_definition]
        else:
            batch_definition_list = self._data_connector.get_batch_definition_list(
                batch_request=batch_request
            )
        return batch_definition_list

    @singledispatchmethod
    def _get_dataframe_partitioner(self, partitioner) -> Optional[DataframePartitioner]: ...

    @_get_dataframe_partitioner.register
    def _(self, partitioner: ColumnPartitionerYearly) -> DataframePartitionerYearly:
        return DataframePartitionerYearly(**partitioner.dict(exclude={"param_names"}))

    @_get_dataframe_partitioner.register
    def _(self, partitioner: ColumnPartitionerMonthly) -> DataframePartitionerMonthly:
        return DataframePartitionerMonthly(**partitioner.dict(exclude={"param_names"}))

    @_get_dataframe_partitioner.register
    def _(self, partitioner: ColumnPartitionerDaily) -> DataframePartitionerDaily:
        return DataframePartitionerDaily(**partitioner.dict(exclude={"param_names"}))

    @_get_dataframe_partitioner.register
    def _(self, partitioner: None) -> None:
        return None

    @override
    def _get_reader_options_include(self) -> set[str]:
        return {
            "data_directory",
        }

    @override
    def get_batch_parameters_keys(
        self,
        partitioner: Optional[ColumnPartitioner] = None,
    ) -> tuple[str, ...]:
        option_keys: tuple[str, ...] = (FILE_PATH_BATCH_SPEC_KEY,)
        dataframe_partitioner = self._get_dataframe_partitioner(partitioner)
        if dataframe_partitioner:
            option_keys += tuple(dataframe_partitioner.param_names)
        return option_keys

    @override
    def get_whole_directory_path_override(
        self,
    ) -> PathStr:
        return self.data_directory

    @override
    def build_batch_request(
        self,
        options: Optional[BatchParameters] = None,
        batch_slice: Optional[BatchSlice] = None,
        partitioner: Optional[ColumnPartitioner] = None,
    ) -> BatchRequest:
        if options is not None and not self._batch_parameters_are_valid(
            options=options,
            partitioner=partitioner,
        ):
            allowed_keys = set(self.get_batch_parameters_keys(partitioner=partitioner))
            actual_keys = set(options.keys())
            raise gx_exceptions.InvalidBatchRequestError(  # noqa: TRY003
                "Batch parameters should only contain keys from the following set:\n"
                f"{allowed_keys}\nbut your specified keys contain\n"
                f"{actual_keys.difference(allowed_keys)}\nwhich is not valid.\n"
            )

        return BatchRequest(
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            options=options or {},
            batch_slice=batch_slice,
            partitioner=partitioner,
        )

    @override
    def _batch_spec_options_from_batch_request(self, batch_request: BatchRequest) -> dict:
        """Build a set of options for use in a batch spec from a batch request.

        Args:
            batch_request: Batch request to use to generate options.

        Returns:
            Dictionary containing batch spec options.
        """
        get_reader_options_include: set[str] | None = self._get_reader_options_include()
        if not get_reader_options_include:
            # Set to None if empty set to include any additional `extra_kwargs` passed to `add_*_asset`  # noqa: E501
            get_reader_options_include = None
        batch_spec_options = {
            "reader_method": self._get_reader_method(),
            "reader_options": self.dict(
                include=get_reader_options_include,
                exclude=self._EXCLUDE_FROM_READER_OPTIONS,
                exclude_unset=True,
                by_alias=True,
                config_provider=self._datasource._config_provider,
            ),
        }

        partitioner_parameters = self._get_partitioner_parameters(batch_request=batch_request)
        if partitioner_parameters:
            batch_spec_options.update(partitioner_parameters)

        return batch_spec_options

    def _get_partitioner_parameters(self, batch_request: BatchRequest) -> Optional[dict]:
        """If a partitioner is present, add its configuration to batch parameters."""
        partitioner: Optional[DataframePartitioner] = self._get_dataframe_partitioner(
            batch_request.partitioner
        )
        if not partitioner:
            return None
        batch_identifiers = partitioner.batch_parameters_to_batch_spec_kwarg_identifiers(
            parameters=batch_request.options
        )
        return {
            "partitioner_method": partitioner.method_name,
            "partitioner_kwargs": {
                **partitioner.partitioner_method_kwargs(),
                "batch_identifiers": batch_identifiers,
            },
        }

    @override
    def _get_sortable_partitioner(
        self, partitioner: Optional[ColumnPartitioner]
    ) -> Optional[PartitionerSortingProtocol]:
        # DirectoryAssets can only ever return a single batch, so they do not require sorting.
        return None
