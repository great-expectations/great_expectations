from __future__ import annotations

import logging
import pathlib
from typing import TYPE_CHECKING

from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.batch import LegacyBatchDefinition
from great_expectations.core.id_dict import IDDict
from great_expectations.datasource.fluent.constants import (
    _DATA_CONNECTOR_NAME,
)
from great_expectations.datasource.fluent.file_path_data_asset import _FilePathDataAsset

if TYPE_CHECKING:
    from great_expectations.alias_types import PathStr
    from great_expectations.datasource.fluent.batch_request import (
        BatchRequest,
    )

logger = logging.getLogger(__name__)


class _DirectoryDataAssetMixin(_FilePathDataAsset):
    """Used for accessing all the files in a directory as a single batch."""

    data_directory: pathlib.Path

    @override
    def _get_batch_definition_list(
        self, batch_request: BatchRequest
    ) -> list[LegacyBatchDefinition]:
        """Generate a batch definition list from a given batch request, handling a partitioner config if present.

        Args:
            batch_request: Batch request used to generate batch definitions.

        Returns:
            List of batch definitions, in the case of a _DirectoryDataAssetMixin the list contains a single item.
        """  # noqa: E501
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

    @override
    def get_whole_directory_path_override(
        self,
    ) -> PathStr:
        return self.data_directory

    @override
    def _get_reader_method(self) -> str:
        raise NotImplementedError(
            """One needs to explicitly provide "reader_method" for File-Path style DataAsset extensions as temporary \
work-around, until "type" naming convention and method for obtaining 'reader_method' from it are established."""  # noqa: E501
        )

    @override
    def _get_reader_options_include(self) -> set[str]:
        return {
            "data_directory",
        }
