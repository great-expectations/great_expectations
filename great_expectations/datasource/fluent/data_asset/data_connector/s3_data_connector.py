from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Callable, ClassVar, List, Optional, Type

import pydantic

from great_expectations.core.batch_spec import PathBatchSpec, S3BatchSpec
from great_expectations.datasource.data_connector.util import (
    list_s3_keys,
    sanitize_prefix_for_gcs_and_s3,
)
from great_expectations.datasource.fluent.data_asset.data_connector import (
    FilePathDataConnector,
)

if TYPE_CHECKING:
    from botocore.client import BaseClient

    from great_expectations.core.batch import BatchDefinition


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
        batching_regex: A regex pattern for partitioning data references
        prefix (str): S3 prefix
        delimiter (str): S3 delimiter
        max_keys (int): S3 max_keys (default is 1000)
        recursive_file_discovery (bool): Flag to indicate if files should be searched recursively from subfolders
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters (list): optional list of sorters for sorting data_references
        # TODO: <Alex>ALEX</Alex>
        file_path_template_map_fn: Format function mapping path to fully-qualified resource on S3
    """

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
        batching_regex: re.Pattern,
        s3_client: BaseClient,
        bucket: str,
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
        recursive_file_discovery: bool = False,
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters: Optional[list] = None,
        # TODO: <Alex>ALEX</Alex>
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
            batching_regex=re.compile(
                f"{re.escape(self._sanitized_prefix)}{batching_regex.pattern}"
            ),
            # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
            # TODO: <Alex>ALEX</Alex>
            # sorters=sorters,
            # TODO: <Alex>ALEX</Alex>
            file_path_template_map_fn=file_path_template_map_fn,
        )

    @classmethod
    def build_data_connector(  # noqa: PLR0913
        cls,
        datasource_name: str,
        data_asset_name: str,
        batching_regex: re.Pattern,
        s3_client: BaseClient,
        bucket: str,
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
        recursive_file_discovery: bool = False,
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters: Optional[list] = None,
        # TODO: <Alex>ALEX</Alex>
        file_path_template_map_fn: Optional[Callable] = None,
    ) -> S3DataConnector:
        """Builds "S3DataConnector", which links named DataAsset to AWS S3.

        Args:
            datasource_name: The name of the Datasource associated with this "S3DataConnector" instance
            data_asset_name: The name of the DataAsset using this "S3DataConnector" instance
            batching_regex: A regex pattern for partitioning data references
            s3_client: S3 Client reference handle
            bucket: bucket for S3
            prefix: S3 prefix
            delimiter: S3 delimiter
            max_keys: S3 max_keys (default is 1000)
            recursive_file_discovery: Flag to indicate if files should be searched recursively from subfolders
            # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
            # TODO: <Alex>ALEX</Alex>
            # sorters: optional list of sorters for sorting data_references
            # TODO: <Alex>ALEX</Alex>
            file_path_template_map_fn: Format function mapping path to fully-qualified resource on S3

        Returns:
            Instantiated "S3DataConnector" object
        """
        return S3DataConnector(
            datasource_name=datasource_name,
            data_asset_name=data_asset_name,
            batching_regex=batching_regex,
            s3_client=s3_client,
            bucket=bucket,
            prefix=prefix,
            delimiter=delimiter,
            max_keys=max_keys,
            recursive_file_discovery=recursive_file_discovery,
            # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
            # TODO: <Alex>ALEX</Alex>
            # sorters=sorters,
            # TODO: <Alex>ALEX</Alex>
            file_path_template_map_fn=file_path_template_map_fn,
        )

    @classmethod
    def build_test_connection_error_message(  # noqa: PLR0913
        cls,
        data_asset_name: str,
        batching_regex: re.Pattern,
        bucket: str,
        prefix: str = "",
        delimiter: str = "/",
        recursive_file_discovery: bool = False,
    ) -> str:
        """Builds helpful error message for reporting issues when linking named DataAsset to Microsoft Azure Blob Storage.

        Args:
            data_asset_name: The name of the DataAsset using this "AzureBlobStorageDataConnector" instance
            batching_regex: A regex pattern for partitioning data references
            bucket: bucket for S3
            prefix: S3 prefix
            delimiter: S3 delimiter
            recursive_file_discovery: Flag to indicate if files should be searched recursively from subfolders

        Returns:
            Customized error message
        """
        test_connection_error_message_template: str = 'No file in bucket "{bucket}" with prefix "{prefix}" and recursive file discovery set to "{recursive_file_discovery}" matched regular expressions pattern "{batching_regex}" using delimiter "{delimiter}" for DataAsset "{data_asset_name}".'
        return test_connection_error_message_template.format(
            **{
                "data_asset_name": data_asset_name,
                "batching_regex": batching_regex.pattern,
                "bucket": bucket,
                "prefix": prefix,
                "delimiter": delimiter,
                "recursive_file_discovery": recursive_file_discovery,
            }
        )

    def build_batch_spec(self, batch_definition: BatchDefinition) -> S3BatchSpec:
        """
        Build BatchSpec from batch_definition by calling DataConnector's build_batch_spec function.

        Args:
            batch_definition (BatchDefinition): to be used to build batch_spec

        Returns:
            BatchSpec built from batch_definition
        """
        batch_spec: PathBatchSpec = super().build_batch_spec(
            batch_definition=batch_definition
        )
        return S3BatchSpec(batch_spec)

    # Interface Method
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
    def _get_full_file_path(self, path: str) -> str:
        if self._file_path_template_map_fn is None:
            raise ValueError(
                f"""Converting file paths to fully-qualified object references for "{self.__class__.__name__}" \
requires "file_path_template_map_fn: Callable" to be set.
"""
            )

        template_arguments: dict = {
            "bucket": self._bucket,
            "path": path,
        }

        return self._file_path_template_map_fn(**template_arguments)
