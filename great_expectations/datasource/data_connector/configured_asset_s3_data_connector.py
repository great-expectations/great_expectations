import logging
import os
from typing import List, Optional

try:
    import boto3
except ImportError:
    boto3 = None

from great_expectations.datasource.data_connector import (
    ConfiguredAssetFilePathDataConnector,
)
from great_expectations.datasource.data_connector.asset import Asset
from great_expectations.datasource.data_connector.util import list_s3_keys
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


class ConfiguredAssetS3DataConnector(ConfiguredAssetFilePathDataConnector):
    def __init__(
        self,
        name: str,
        datasource_name: str,
        bucket: str,
        assets: dict,
        execution_engine: Optional[ExecutionEngine] = None,
        default_regex: Optional[dict] = None,
        sorters: Optional[list] = None,
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
        boto3_options: dict = None,
    ):
        logger.debug(f'Constructing ConfiguredAssetS3DataConnector "{name}".')

        super().__init__(
            name=name,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
            assets=assets,
            default_regex=default_regex,
            sorters=sorters,
        )
        self._bucket = bucket
        self._prefix = os.path.join(prefix, "")
        self._delimiter = delimiter
        self._max_keys = max_keys

        if boto3_options is None:
            boto3_options = {}

        try:
            self._s3 = boto3.client("s3", **boto3_options)
        except (TypeError, AttributeError):
            raise ImportError(
                "Unable to load boto3 (it is required for ConfiguredAssetS3DataConnector)."
            )

    def _get_data_reference_list_for_asset(self, asset: Optional[Asset]) -> List[str]:
        query_options: dict = {
            "Bucket": self._bucket,
            "Prefix": self._prefix,
            "Delimiter": self._delimiter,
            "MaxKeys": self._max_keys,
        }
        if asset is not None:
            if asset.bucket:
                query_options["Bucket"] = asset.bucket
            if asset.prefix:
                query_options["Prefix"] = asset.prefix
            if asset.delimiter:
                query_options["Delimiter"] = asset.delimiter
            if asset.max_keys:
                query_options["MaxKeys"] = asset.max_keys

        path_list: List[str] = [
            key
            for key in list_s3_keys(
                s3=self._s3,
                query_options=query_options,
                iterator_dict={},
                recursive=False,
            )
        ]
        return path_list

    def _get_full_file_path(
        self, path: str, data_asset_name: Optional[str] = None,
    ) -> str:
        # data_assert_name isn't used in this method.
        # It's only kept for compatibility with parent methods.
        return f"s3a://{os.path.join(self._bucket, path)}"
