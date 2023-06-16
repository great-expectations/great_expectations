from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Callable, ClassVar, List, Optional, Type

import pydantic

from great_expectations.core.batch_spec import AzureBatchSpec, PathBatchSpec
from great_expectations.datasource.data_connector.util import (
    list_azure_keys,
    sanitize_prefix,
)
from great_expectations.datasource.fluent.data_asset.data_connector import (
    FilePathDataConnector,
)

if TYPE_CHECKING:
    from great_expectations.compatibility import azure
    from great_expectations.core.batch import BatchDefinition


logger = logging.getLogger(__name__)


class _AzureOptions(pydantic.BaseModel):
    abs_container: str
    abs_name_starts_with: str = ""
    abs_delimiter: str = "/"
    abs_recursive_file_discovery: bool = False


class AzureBlobStorageDataConnector(FilePathDataConnector):
    """Extension of FilePathDataConnector used to connect to Microsoft Azure Blob Storage (ABS).

    Args:
        datasource_name: The name of the Datasource associated with this DataConnector instance
        data_asset_name: The name of the DataAsset using this DataConnector instance
        batching_regex: A regex pattern for partitioning data references
        azure_client: Reference to instantiated Microsoft Azure Blob Storage client handle
        account_name (str): account name for Microsoft Azure Blob Storage
        container (str): container name for Microsoft Azure Blob Storage
        name_starts_with (str): Microsoft Azure Blob Storage prefix
        delimiter (str): Microsoft Azure Blob Storage delimiter
        recursive_file_discovery (bool): Flag to indicate if files should be searched recursively from subfolders
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters (list): optional list of sorters for sorting data_references
        # TODO: <Alex>ALEX</Alex>
        file_path_template_map_fn: Format function mapping path to fully-qualified resource on ABS
    """

    asset_level_option_keys: ClassVar[tuple[str, ...]] = (
        "abs_container",
        "abs_name_starts_with",
        "abs_delimiter",
        "abs_recursive_file_discovery",
    )
    asset_options_type: ClassVar[Type[_AzureOptions]] = _AzureOptions

    def __init__(  # noqa: PLR0913
        self,
        datasource_name: str,
        data_asset_name: str,
        batching_regex: re.Pattern,
        azure_client: azure.BlobServiceClient,
        account_name: str,
        container: str,
        name_starts_with: str = "",
        delimiter: str = "/",
        recursive_file_discovery: bool = False,
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters: Optional[list] = None,
        # TODO: <Alex>ALEX</Alex>
        file_path_template_map_fn: Optional[Callable] = None,
    ) -> None:
        self._azure_client: azure.BlobServiceClient = azure_client

        self._account_name = account_name
        self._container = container

        self._prefix: str = name_starts_with
        self._sanitized_prefix: str = sanitize_prefix(text=name_starts_with)

        self._delimiter = delimiter

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
        azure_client: azure.BlobServiceClient,
        account_name: str,
        container: str,
        name_starts_with: str = "",
        delimiter: str = "/",
        recursive_file_discovery: bool = False,
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters: Optional[list] = None,
        # TODO: <Alex>ALEX</Alex>
        file_path_template_map_fn: Optional[Callable] = None,
    ) -> AzureBlobStorageDataConnector:
        """Builds "AzureBlobStorageDataConnector", which links named DataAsset to Microsoft Azure Blob Storage.

        Args:
            datasource_name: The name of the Datasource associated with this "AzureBlobStorageDataConnector" instance
            data_asset_name: The name of the DataAsset using this "AzureBlobStorageDataConnector" instance
            batching_regex: A regex pattern for partitioning data references
            azure_client: Reference to instantiated Microsoft Azure Blob Storage client handle
            account_name: account name for Microsoft Azure Blob Storage
            container: container name for Microsoft Azure Blob Storage
            name_starts_with: Microsoft Azure Blob Storage prefix
            delimiter: Microsoft Azure Blob Storage delimiter
            recursive_file_discovery: Flag to indicate if files should be searched recursively from subfolders
            # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
            # TODO: <Alex>ALEX</Alex>
            # sorters: optional list of sorters for sorting data_references
            # TODO: <Alex>ALEX</Alex>
            file_path_template_map_fn: Format function mapping path to fully-qualified resource on ABS

        Returns:
            Instantiated "AzureBlobStorageDataConnector" object
        """
        return AzureBlobStorageDataConnector(
            datasource_name=datasource_name,
            data_asset_name=data_asset_name,
            batching_regex=batching_regex,
            azure_client=azure_client,
            account_name=account_name,
            container=container,
            name_starts_with=name_starts_with,
            delimiter=delimiter,
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
        account_name: str,
        container: str,
        name_starts_with: str = "",
        delimiter: str = "/",
        recursive_file_discovery: bool = False,
    ) -> str:
        """Builds helpful error message for reporting issues when linking named DataAsset to Microsoft Azure Blob Storage.

        Args:
            data_asset_name: The name of the DataAsset using this "AzureBlobStorageDataConnector" instance
            batching_regex: A regex pattern for partitioning data references
            account_name: account name for Microsoft Azure Blob Storage
            container: container name for Microsoft Azure Blob Storage
            name_starts_with: Microsoft Azure Blob Storage prefix
            delimiter: Microsoft Azure Blob Storage delimiter
            recursive_file_discovery: Flag to indicate if files should be searched recursively from subfolders

        Returns:
            Customized error message
        """
        test_connection_error_message_template: str = 'No file belonging to account "{account_name}" in container "{container}" with prefix "{name_starts_with}" and recursive file discovery set to "{recursive_file_discovery}" matched regular expressions pattern "{batching_regex}" using delimiter "{delimiter}" for DataAsset "{data_asset_name}".'
        return test_connection_error_message_template.format(
            **{
                "data_asset_name": data_asset_name,
                "batching_regex": batching_regex.pattern,
                "account_name": account_name,
                "container": container,
                "name_starts_with": name_starts_with,
                "delimiter": delimiter,
                "recursive_file_discovery": recursive_file_discovery,
            }
        )

    def build_batch_spec(self, batch_definition: BatchDefinition) -> AzureBatchSpec:
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
        return AzureBatchSpec(batch_spec)

    # Interface Method
    def get_data_references(self) -> List[str]:
        query_options: dict = {
            "container": self._container,
            "name_starts_with": self._sanitized_prefix,
            "delimiter": self._delimiter,
        }
        path_list: List[str] = list_azure_keys(
            azure_client=self._azure_client,
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
            "account_name": self._account_name,
            "container": self._container,
            "path": path,
        }

        return self._file_path_template_map_fn(**template_arguments)
