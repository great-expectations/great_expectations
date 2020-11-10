import os
from typing import List, Optional

try:
    import boto3
except ImportError:
    boto3 = None

import logging

from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_environment.data_connector import InferredAssetFilePathDataConnector
import great_expectations.exceptions as ge_exceptions

logger = logging.getLogger(__name__)


# TODO: <Alex>Clean up order of arguments.</Alex>
class InferredAssetS3DataConnector(InferredAssetFilePathDataConnector):
    def __init__(
        self,
        name: str,
        execution_environment_name: str,
        bucket: str,
        default_regex: dict = None,
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
            "Delimiter": self._delimiter,
            "Prefix": self._prefix,
            "MaxKeys": self._max_keys,
        }
        path_list: List[str] = [
            key for key in self._list_s3_keys(query_options=query_options, iterator_dict={})
        ]
        return path_list

    def _get_full_file_path(self, path: str) -> str:
        return f"s3a://{os.path.join(self._bucket, path)}"

    def _list_s3_keys(self, query_options, iterator_dict: dict) -> str:
        if iterator_dict is None:
            iterator_dict = {}

        if "continuation_token" in iterator_dict:
            query_options.update(
                {"ContinuationToken": iterator_dict["continuation_token"]}
            )

        logger.debug(f"Fetching objects from S3 with query options: {query_options}")

        s3_objects_info: dict = self._s3.list_objects_v2(**query_options)

        if "Contents" not in s3_objects_info:
            raise ge_exceptions.DataConnectorError("S3 query may not be configured correctly.")

        keys: List[str] = [
            item["Key"] for item in s3_objects_info["Contents"] if item["Size"] > 0
        ]

        yield from keys

        if s3_objects_info["IsTruncated"]:
            iterator_dict["continuation_token"] = s3_objects_info["NextContinuationToken"]
            # Recursively fetch more
            yield from self._list_s3_keys(iterator_dict=iterator_dict)
        elif "continuation_token" in iterator_dict:
            # Make sure we clear the token once we've gotten fully through
            del iterator_dict["continuation_token"]
