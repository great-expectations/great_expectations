import os
from typing import List, Optional

try:
    import boto3
except ImportError:
    boto3 = None

import logging

from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_environment.data_connector import InferredAssetFilePathDataConnector
from great_expectations.execution_environment.data_connector.util import list_s3_keys

logger = logging.getLogger(__name__)


# TODO: <Alex>Clean up order of arguments.</Alex>
class InferredAssetS3DataConnector(InferredAssetFilePathDataConnector):
    def __init__(
        self,
        name: str,
        execution_environment_name: str,
        bucket: str,
        default_regex: dict,
        execution_engine: ExecutionEngine = None,
        sorters: list = None,
        data_context_root_directory: str = None,
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
        boto3_options: dict = None
    ):
        logger.debug(f'Constructing InferredAssetS3DataConnector "{name}".')

        super().__init__(
            name=name,
            execution_environment_name=execution_environment_name,
            execution_engine=execution_engine,
            default_regex=default_regex,
            sorters=sorters,
            data_context_root_directory=data_context_root_directory
        )

        self._bucket = bucket
        self._prefix = os.path.join(prefix, "")
        self._delimiter = delimiter
        self._max_keys = max_keys

        if boto3_options is None:
            boto3_options = {}

        try:
            self._s3 = boto3.client("s3", **boto3_options)
        except TypeError:
            raise ImportError("Unable to load boto3 (it is required for InferredAssetS3DataConnector).")

    def _get_data_reference_list(self, data_asset_name: Optional[str] = None) -> List[str]:
        """List objects in the underlying data store to create a list of data_references.

        This method is used to refresh the cache.
        """
        query_options: dict = {
            "Bucket": self._bucket,
            "Prefix": self._prefix,
            "Delimiter": self._delimiter,
            "MaxKeys": self._max_keys,
        }
        path_list: List[str] = [
            key for key in list_s3_keys(s3=self._s3, query_options=query_options, iterator_dict={}, recursive=True)
        ]
        return path_list

    def _get_full_file_path(self, path: str) -> str:
        return f"s3a://{os.path.join(self._bucket, path)}"
