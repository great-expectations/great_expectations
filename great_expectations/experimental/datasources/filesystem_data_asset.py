from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Optional, Set

import great_expectations.exceptions as ge_exceptions
from great_expectations.experimental.datasources.data_asset.data_connector import (
    FilesystemDataConnector,
)
from great_expectations.experimental.datasources.file_path_data_asset import (
    _FilePathDataAsset,
)

if TYPE_CHECKING:
    from great_expectations.experimental.datasources.interfaces import (
        BatchRequest,
        BatchRequestOptions,
    )

logger = logging.getLogger(__name__)


class _FilesystemDataAsset(_FilePathDataAsset):
    _EXCLUDE_FROM_READER_OPTIONS: ClassVar[
        Set[str]
    ] = _FilePathDataAsset._EXCLUDE_FROM_READER_OPTIONS | {
        "glob_directive",
    }

    # Filesystem specific attributes
    glob_directive: str = "**/*"

    def batch_request_options_template(
        self,
    ) -> BatchRequestOptions:
        idx: int
        return {idx: None for idx in self._all_group_names}

    def build_batch_request(
        self, options: Optional[BatchRequestOptions] = None
    ) -> BatchRequest:
        if options:
            for option, value in options.items():
                if (
                    option in self._all_group_name_to_group_index_mapping
                    and not isinstance(value, str)
                ):
                    raise ge_exceptions.InvalidBatchRequestError(
                        f"All regex matching options must be strings. The value of '{option}' is "
                        f"not a string: {value}"
                    )

        return super().build_batch_request(options)

    def _build_data_connector(self) -> None:
        self._data_connector = FilesystemDataConnector(
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            base_directory=self.datasource.base_directory,
            regex=self.regex,
            glob_directive=self.glob_directive,
            data_context_root_directory=self.datasource.data_context_root_directory,
        )
