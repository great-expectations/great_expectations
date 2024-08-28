from __future__ import annotations

import copy
import logging
import re
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Dict, Generator, List, Optional, Type

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.batch_spec import PathBatchSpec, S3BatchSpec
from great_expectations.datasource.fluent.data_connector import (
    FilePathDataConnector,
)
from great_expectations.datasource.fluent.data_connector.file_path_data_connector import (
    sanitize_prefix_for_gcs_and_s3,
)

if TYPE_CHECKING:
    from botocore.client import BaseClient

    from great_expectations.core.batch import LegacyBatchDefinition


logger = logging.getLogger(__name__)


class _S3Options(pydantic.BaseModel):
    s3_prefix: str = ""
    s3_delimiter: str = "/"
    s3_max_keys: int = 1000
    s3_recursive_file_discovery: bool = False


class S3DataConnector(FilePathDataConnector):
    """Extension of FilePathDataConnector used to connect to S3.


    Args:
        datasource_name: The name of the Datasource associated with this DataConnector instance
        data_asset_name: The name of the DataAsset using this DataConnector instance
        s3_client: Reference to instantiated AWS S3 client handle
        bucket (str): bucket for S3
        prefix (str): S3 prefix
        delimiter (str): S3 delimiter
        max_keys (int): S3 max_keys (default is 1000)
        recursive_file_discovery (bool): Flag to indicate if files should be searched recursively from subfolders
        file_path_template_map_fn: Format function mapping path to fully-qualified resource on S3
    """  # noqa: E501

    asset_level_option_keys: ClassVar[tuple[str, ...]] = (
        "s3_prefix",
        "s3_delimiter",
        "s3_max_keys",
        "s3_recursive_file_discovery",
    )
    asset_options_type: ClassVar[Type[_S3Options]] = _S3Options

    def __init__(  # noqa: PLR0913
        self,
        datasource_name: str,
        data_asset_name: str,
        s3_client: BaseClient,
        bucket: str,
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
        recursive_file_discovery: bool = False,
        file_path_template_map_fn: Optional[Callable] = None,
    ) -> None:
        self._s3_client: BaseClient = s3_client

        self._bucket: str = bucket

        self._prefix: str = prefix
        self._sanitized_prefix: str = sanitize_prefix_for_gcs_and_s3(text=prefix)

        self._delimiter: str = delimiter
        self._max_keys: int = max_keys

        self._recursive_file_discovery = recursive_file_discovery

        super().__init__(
            datasource_name=datasource_name,
            data_asset_name=data_asset_name,
            file_path_template_map_fn=file_path_template_map_fn,
        )

    @classmethod
    def build_data_connector(  # noqa: PLR0913
        cls,
        datasource_name: str,
        data_asset_name: str,
        s3_client: BaseClient,
        bucket: str,
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
        recursive_file_discovery: bool = False,
        file_path_template_map_fn: Optional[Callable] = None,
    ) -> S3DataConnector:
        """Builds "S3DataConnector", which links named DataAsset to AWS S3.

        Args:
            datasource_name: The name of the Datasource associated with this "S3DataConnector" instance
            data_asset_name: The name of the DataAsset using this "S3DataConnector" instance
            s3_client: S3 Client reference handle
            bucket: bucket for S3
            prefix: S3 prefix
            delimiter: S3 delimiter
            max_keys: S3 max_keys (default is 1000)
            recursive_file_discovery: Flag to indicate if files should be searched recursively from subfolders
            file_path_template_map_fn: Format function mapping path to fully-qualified resource on S3

        Returns:
            Instantiated "S3DataConnector" object
        """  # noqa: E501
        return S3DataConnector(
            datasource_name=datasource_name,
            data_asset_name=data_asset_name,
            s3_client=s3_client,
            bucket=bucket,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
            recursive_file_discovery=recursive_file_discovery,
            file_path_template_map_fn=file_path_template_map_fn,
        )

    @classmethod
    def build_test_connection_error_message(
        cls,
        data_asset_name: str,
        bucket: str,
        prefix: str = "",
        delimiter: str = "/",
        recursive_file_discovery: bool = False,
    ) -> str:
        """Builds helpful error message for reporting issues when linking named DataAsset to Microsoft Azure Blob Storage.

        Args:
            data_asset_name: The name of the DataAsset using this "AzureBlobStorageDataConnector" instance
            bucket: bucket for S3
            prefix: S3 prefix
            delimiter: S3 delimiter
            recursive_file_discovery: Flag to indicate if files should be searched recursively from subfolders

        Returns:
            Customized error message
        """  # noqa: E501
        test_connection_error_message_template: str = 'No file in bucket "{bucket}" with prefix "{prefix}" and recursive file discovery set to "{recursive_file_discovery}" found using delimiter "{delimiter}" for DataAsset "{data_asset_name}".'  # noqa: E501
        return test_connection_error_message_template.format(
            **{
                "data_asset_name": data_asset_name,
                "bucket": bucket,
                "prefix": prefix,
                "delimiter": delimiter,
                "recursive_file_discovery": recursive_file_discovery,
            }
        )

    @override
    def build_batch_spec(self, batch_definition: LegacyBatchDefinition) -> S3BatchSpec:
        """
        Build BatchSpec from batch_definition by calling DataConnector's build_batch_spec function.

        Args:
            batch_definition (LegacyBatchDefinition): to be used to build batch_spec

        Returns:
            BatchSpec built from batch_definition
        """
        batch_spec: PathBatchSpec = super().build_batch_spec(batch_definition=batch_definition)
        return S3BatchSpec(batch_spec)

    # Interface Method
    @override
    def get_data_references(self) -> List[str]:
        query_options: dict = {
            "Bucket": self._bucket,
            "Prefix": self._sanitized_prefix,
            "Delimiter": self._delimiter,
            "MaxKeys": self._max_keys,
        }
        path_list: List[str] = list(
            list_s3_keys(
                s3=self._s3_client,
                query_options=query_options,
                iterator_dict={},
                recursive=self._recursive_file_discovery,
            )
        )
        return path_list

    # Interface Method
    @override
    def _get_full_file_path(self, path: str) -> str:
        if self._file_path_template_map_fn is None:
            raise ValueError(  # noqa: TRY003
                f"""Converting file paths to fully-qualified object references for "{self.__class__.__name__}" \
requires "file_path_template_map_fn: Callable" to be set.
"""  # noqa: E501
            )

        template_arguments: dict = {
            "bucket": self._bucket,
            "path": path,
        }

        return self._file_path_template_map_fn(**template_arguments)

    @override
    def _preprocess_batching_regex(self, regex: re.Pattern) -> re.Pattern:
        regex = re.compile(f"{re.escape(self._sanitized_prefix)}{regex.pattern}")
        return super()._preprocess_batching_regex(regex=regex)


def list_s3_keys(  # noqa: C901 - too complex
    s3, query_options: dict, iterator_dict: dict, recursive: bool = False
) -> Generator[str, None, None]:
    """
    For InferredAssetS3DataConnector, we take bucket and prefix and search for files using RegEx at and below the level
    specified by that bucket and prefix.  However, for ConfiguredAssetS3DataConnector, we take bucket and prefix and
    search for files using RegEx only at the level specified by that bucket and prefix.  This restriction for the
    ConfiguredAssetS3DataConnector is needed, because paths on S3 are comprised not only the leaf file name but the
    full path that includes both the prefix and the file name.  Otherwise, in the situations where multiple data assets
    share levels of a directory tree, matching files to data assets will not be possible, due to the path ambiguity.
    :param s3: s3 client connection
    :param query_options: s3 query attributes ("Bucket", "Prefix", "Delimiter", "MaxKeys")
    :param iterator_dict: dictionary to manage "NextContinuationToken" (if "IsTruncated" is returned from S3)
    :param recursive: True for InferredAssetS3DataConnector and False for ConfiguredAssetS3DataConnector (see above)
    :return: string valued key representing file path on S3 (full prefix and leaf file name)
    """  # noqa: E501
    if iterator_dict is None:
        iterator_dict = {}

    if "continuation_token" in iterator_dict:
        query_options.update({"ContinuationToken": iterator_dict["continuation_token"]})

    logger.debug(f"Fetching objects from S3 with query options: {query_options}")

    s3_objects_info: dict = s3.list_objects_v2(**query_options)

    if not any(key in s3_objects_info for key in ["Contents", "CommonPrefixes"]):
        raise ValueError("S3 query may not have been configured correctly.")  # noqa: TRY003

    if "Contents" in s3_objects_info:
        keys: List[str] = [item["Key"] for item in s3_objects_info["Contents"] if item["Size"] > 0]
        yield from keys

    if recursive and "CommonPrefixes" in s3_objects_info:
        common_prefixes: List[Dict[str, Any]] = s3_objects_info["CommonPrefixes"]
        for prefix_info in common_prefixes:
            query_options_tmp: dict = copy.deepcopy(query_options)
            query_options_tmp.update({"Prefix": prefix_info["Prefix"]})
            # Recursively fetch from updated prefix
            yield from list_s3_keys(
                s3=s3,
                query_options=query_options_tmp,
                iterator_dict={},
                recursive=recursive,
            )

    if s3_objects_info["IsTruncated"]:
        iterator_dict["continuation_token"] = s3_objects_info["NextContinuationToken"]
        # Recursively fetch more
        yield from list_s3_keys(
            s3=s3,
            query_options=query_options,
            iterator_dict=iterator_dict,
            recursive=recursive,
        )

    if "continuation_token" in iterator_dict:
        # Make sure we clear the token once we've gotten fully through
        del iterator_dict["continuation_token"]
