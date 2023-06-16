from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Callable, ClassVar, List, Optional, Type

import pydantic

from great_expectations.core.batch_spec import GCSBatchSpec, PathBatchSpec
from great_expectations.datasource.data_connector.util import (
    list_gcs_keys,
    sanitize_prefix_for_gcs_and_s3,
)
from great_expectations.datasource.fluent.data_asset.data_connector import (
    FilePathDataConnector,
)

if TYPE_CHECKING:
    from great_expectations.compatibility import google
    from great_expectations.core.batch import BatchDefinition


logger = logging.getLogger(__name__)


class _GCSOptions(pydantic.BaseModel):
    gcs_prefix: str = ""
    gcs_delimiter: str = "/"
    gcs_max_results: int = 1000
    gcs_recursive_file_discovery: bool = False


class GoogleCloudStorageDataConnector(FilePathDataConnector):
    """Extension of FilePathDataConnector used to connect to Google Cloud Storage (GCS).

    Args:
        datasource_name: The name of the Datasource associated with this DataConnector instance
        data_asset_name: The name of the DataAsset using this DataConnector instance
        batching_regex: A regex pattern for partitioning data references
        gcs_client: Reference to instantiated Google Cloud Storage client handle
        bucket_or_name (str): bucket name for Google Cloud Storage
        prefix (str): GCS prefix
        delimiter (str): GCS delimiter
        max_results (int): max blob filepaths to return
        recursive_file_discovery (bool): Flag to indicate if files should be searched recursively from subfolders
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters (list): optional list of sorters for sorting data_references
        # TODO: <Alex>ALEX</Alex>
        file_path_template_map_fn: Format function mapping path to fully-qualified resource on GCS
    """

    asset_level_option_keys: ClassVar[tuple[str, ...]] = (
        "gcs_prefix",
        "gcs_delimiter",
        "gcs_max_results",
        "gcs_recursive_file_discovery",
    )
    asset_options_type: ClassVar[Type[_GCSOptions]] = _GCSOptions

    def __init__(  # noqa: PLR0913
        self,
        datasource_name: str,
        data_asset_name: str,
        batching_regex: re.Pattern,
        # TODO: <Alex>ALEX</Alex>
        gcs_client: google.Client,
        bucket_or_name: str,
        prefix: str = "",
        delimiter: str = "/",
        max_results: Optional[int] = None,
        recursive_file_discovery: bool = False,
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters: Optional[list] = None,
        # TODO: <Alex>ALEX</Alex>
        file_path_template_map_fn: Optional[Callable] = None,
    ) -> None:
        self._gcs_client: google.Client = gcs_client

        self._bucket_or_name = bucket_or_name

        self._prefix: str = prefix
        self._sanitized_prefix: str = sanitize_prefix_for_gcs_and_s3(text=prefix)

        self._delimiter = delimiter
        self._max_results = max_results

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
        gcs_client: google.Client,
        bucket_or_name: str,
        prefix: str = "",
        delimiter: str = "/",
        max_results: Optional[int] = None,
        recursive_file_discovery: bool = False,
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters: Optional[list] = None,
        # TODO: <Alex>ALEX</Alex>
        file_path_template_map_fn: Optional[Callable] = None,
    ) -> GoogleCloudStorageDataConnector:
        """Builds "GoogleCloudStorageDataConnector", which links named DataAsset to Google Cloud Storage.

        Args:
            datasource_name: The name of the Datasource associated with this "GoogleCloudStorageDataConnector" instance
            data_asset_name: The name of the DataAsset using this "GoogleCloudStorageDataConnector" instance
            batching_regex: A regex pattern for partitioning data references
            gcs_client: Reference to instantiated Google Cloud Storage client handle
            bucket_or_name: bucket name for Google Cloud Storage
            prefix: GCS prefix
            delimiter: GCS delimiter
            recursive_file_discovery: Flag to indicate if files should be searched recursively from subfolders
            max_results: max blob filepaths to return
            # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
            # TODO: <Alex>ALEX</Alex>
            # sorters: optional list of sorters for sorting data_references
            # TODO: <Alex>ALEX</Alex>
            file_path_template_map_fn: Format function mapping path to fully-qualified resource on GCS

        Returns:
            Instantiated "GoogleCloudStorageDataConnector" object
        """
        return GoogleCloudStorageDataConnector(
            datasource_name=datasource_name,
            data_asset_name=data_asset_name,
            batching_regex=batching_regex,
            gcs_client=gcs_client,
            bucket_or_name=bucket_or_name,
            prefix=prefix,
            delimiter=delimiter,
            max_results=max_results,
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
        bucket_or_name: str,
        prefix: str = "",
        delimiter: str = "/",
        recursive_file_discovery: bool = False,
    ) -> str:
        """Builds helpful error message for reporting issues when linking named DataAsset to Google Cloud Storage.

        Args:
            data_asset_name: The name of the DataAsset using this "GoogleCloudStorageDataConnector" instance
            batching_regex: A regex pattern for partitioning data references
            bucket_or_name: bucket name for Google Cloud Storage
            prefix: GCS prefix
            delimiter: GCS delimiter
            recursive_file_discovery: Flag to indicate if files should be searched recursively from subfolders

        Returns:
            Customized error message
        """
        test_connection_error_message_template: str = 'No file in bucket "{bucket_or_name}" with prefix "{prefix}" and recursive file discovery set to "{recursive_file_discovery}" matched regular expressions pattern "{batching_regex}" using delimiter "{delimiter}" for DataAsset "{data_asset_name}".'
        return test_connection_error_message_template.format(
            **{
                "data_asset_name": data_asset_name,
                "batching_regex": batching_regex.pattern,
                "bucket_or_name": bucket_or_name,
                "prefix": prefix,
                "delimiter": delimiter,
                "recursive_file_discovery": recursive_file_discovery,
            }
        )

    def build_batch_spec(self, batch_definition: BatchDefinition) -> GCSBatchSpec:
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
        return GCSBatchSpec(batch_spec)

    # Interface Method
    def get_data_references(self) -> List[str]:
        query_options: dict = {
            "bucket_or_name": self._bucket_or_name,
            "prefix": self._sanitized_prefix,
            "delimiter": self._delimiter,
            "max_results": self._max_results,
        }
        path_list: List[str] = list_gcs_keys(
            gcs_client=self._gcs_client,
            query_options=query_options,
            recursive=self._recursive_file_discovery,
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
            "bucket_or_name": self._bucket_or_name,
            "path": path,
        }

        return self._file_path_template_map_fn(**template_arguments)
