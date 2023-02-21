from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Optional, Set

import great_expectations.exceptions as ge_exceptions
from great_expectations.experimental.datasources.data_asset.data_connector import (
    DataConnector,
    S3DataConnector,
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


class _S3DataAsset(_FilePathDataAsset):
    _EXCLUDE_FROM_READER_OPTIONS: ClassVar[
        Set[str]
    ] = _FilePathDataAsset._EXCLUDE_FROM_READER_OPTIONS | {
        "prefix",
        "delimiter",
        "max_keys",
    }

    # S3 specific attributes
    prefix: str = ""
    delimiter: str = "/"
    max_keys: int = 1000

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

    def _build_data_connector(self) -> DataConnector:
        data_connector: DataConnector = S3DataConnector(
            datasource_name=self.datasource.name,
            data_asset_name=self.name,
            s3_client=self.datasource.s3_client,
            bucket=self.datasource.bucket,
            regex=self.regex,
            prefix=self.prefix,
            delimiter=self.delimiter,
            max_keys=self.max_keys,
        )
        return data_connector

    def _build_test_connection_error_message(self) -> str:
        return f"""No object at S3 bucket "{self.datasource.bucket}" matched regular expressions pattern "{self.regex.pattern}" and/or prefix "{self.prefix}"  and/or delimiter "{self.delimiter}" for DataAsset "{self.name}"."""
