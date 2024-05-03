from __future__ import annotations

import pathlib
from abc import ABC
from typing import TYPE_CHECKING, Generic, Optional, Pattern

from great_expectations import exceptions as gx_exceptions
from great_expectations._docs_decorators import public_api
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core import IDDict
from great_expectations.core.batch import LegacyBatchDefinition
from great_expectations.core.partitioners import RegexPartitioner
from great_expectations.datasource.fluent import BatchRequest
from great_expectations.datasource.fluent.constants import _DATA_CONNECTOR_NAME, MATCH_ALL_PATTERN
from great_expectations.datasource.fluent.data_asset.path.path_data_asset import (
    PathDataAsset,
)
from great_expectations.datasource.fluent.data_connector import FILE_PATH_BATCH_SPEC_KEY
from great_expectations.datasource.fluent.interfaces import DatasourceT

if TYPE_CHECKING:
    from great_expectations.alias_types import PathStr
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.datasource.data_connector.batch_filter import BatchSlice
    from great_expectations.datasource.fluent import BatchParameters


class DirectoryDataAsset(PathDataAsset[DatasourceT, RegexPartitioner], Generic[DatasourceT], ABC):
    """Base class for PathDataAssets which batch by combining the contents of a directory."""

    data_directory: pathlib.Path
    # todo: remove. this is included to allow for an incremental refactor.
    batching_regex: Pattern = (  # must use typing.Pattern for pydantic < v1.10
        MATCH_ALL_PATTERN
    )

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
                batch_identifiers=IDDict(batch_identifiers),
            )
            batch_definition_list = [batch_definition]
        else:
            batch_definition_list = self._data_connector.get_batch_definition_list(
                batch_request=batch_request
            )
        return batch_definition_list

    @public_api
    def add_batch_definition_whole_directory(self, name: str) -> BatchDefinition:
        """Add a BatchDefinition which creates a single batch for the entire directory."""
        return self.add_batch_definition(name=name, partitioner=None)

    @override
    def _get_reader_options_include(self) -> set[str]:
        return {
            "data_directory",
        }

    @override
    def get_batch_parameters_keys(
        self,
        partitioner: Optional[RegexPartitioner] = None,
    ) -> tuple[str, ...]:
        option_keys: tuple[str, ...] = (FILE_PATH_BATCH_SPEC_KEY,)
        # todo: need to get dataframe partitioner here
        if partitioner:
            option_keys += tuple(partitioner.param_names)
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
        partitioner: Optional[RegexPartitioner] = None,
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
