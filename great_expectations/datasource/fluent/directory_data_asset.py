from __future__ import annotations

import logging
import pathlib
from typing import TYPE_CHECKING

from great_expectations.core.batch import BatchDefinition
from great_expectations.core.id_dict import IDDict
from great_expectations.datasource.fluent.constants import (
    _DATA_CONNECTOR_NAME,
)
from great_expectations.datasource.fluent.file_path_data_asset import _FilePathDataAsset

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.batch_request import (
        BatchRequest,
    )

logger = logging.getLogger(__name__)


class _DirectoryDataAsset(_FilePathDataAsset):

    data_directory: pathlib.Path

    def _get_batch_definition_list(
        self, batch_request: BatchRequest
    ) -> list[BatchDefinition]:
        if self.splitter:
            # Currently non-sql asset splitters do not introspect the datasource for available
            # batches and only return a single batch based on specified batch_identifiers.
            batch_identifiers = batch_request.options
            if not batch_identifiers.get("path"):
                batch_identifiers["path"] = self.data_directory

            batch_definition = BatchDefinition(
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
