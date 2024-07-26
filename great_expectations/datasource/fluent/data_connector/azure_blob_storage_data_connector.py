from __future__ import annotations

import logging
import os
import re
from typing import TYPE_CHECKING, Callable, ClassVar, List, Optional, Type

from great_expectations.compatibility import azure, pydantic
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.batch_spec import AzureBatchSpec, PathBatchSpec
from great_expectations.datasource.fluent.data_connector import (
    FilePathDataConnector,
)

if TYPE_CHECKING:
    from great_expectations.compatibility import azure
    from great_expectations.core.batch import LegacyBatchDefinition


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
        azure_client: Reference to instantiated Microsoft Azure Blob Storage client handle
        account_name (str): account name for Microsoft Azure Blob Storage
        container (str): container name for Microsoft Azure Blob Storage
        name_starts_with (str): Microsoft Azure Blob Storage prefix
        delimiter (str): Microsoft Azure Blob Storage delimiter
        recursive_file_discovery (bool): Flag to indicate if files should be searched recursively from subfolders
        file_path_template_map_fn: Format function mapping path to fully-qualified resource on ABS
    """  # noqa: E501

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
        azure_client: azure.BlobServiceClient,
        account_name: str,
        container: str,
        name_starts_with: str = "",
        delimiter: str = "/",
        recursive_file_discovery: bool = False,
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
            file_path_template_map_fn=file_path_template_map_fn,
        )

    @classmethod
    def build_data_connector(  # noqa: PLR0913
        cls,
        datasource_name: str,
        data_asset_name: str,
        azure_client: azure.BlobServiceClient,
        account_name: str,
        container: str,
        name_starts_with: str = "",
        delimiter: str = "/",
        recursive_file_discovery: bool = False,
        file_path_template_map_fn: Optional[Callable] = None,
    ) -> AzureBlobStorageDataConnector:
        """Builds "AzureBlobStorageDataConnector", which links named DataAsset to Microsoft Azure Blob Storage.

        Args:
            datasource_name: The name of the Datasource associated with this "AzureBlobStorageDataConnector" instance
            data_asset_name: The name of the DataAsset using this "AzureBlobStorageDataConnector" instance
            azure_client: Reference to instantiated Microsoft Azure Blob Storage client handle
            account_name: account name for Microsoft Azure Blob Storage
            container: container name for Microsoft Azure Blob Storage
            name_starts_with: Microsoft Azure Blob Storage prefix
            delimiter: Microsoft Azure Blob Storage delimiter
            recursive_file_discovery: Flag to indicate if files should be searched recursively from subfolders
            file_path_template_map_fn: Format function mapping path to fully-qualified resource on ABS

        Returns:
            Instantiated "AzureBlobStorageDataConnector" object
        """  # noqa: E501
        return AzureBlobStorageDataConnector(
            datasource_name=datasource_name,
            data_asset_name=data_asset_name,
            azure_client=azure_client,
            account_name=account_name,
            container=container,
            name_starts_with=name_starts_with,
            delimiter=delimiter,
            recursive_file_discovery=recursive_file_discovery,
            file_path_template_map_fn=file_path_template_map_fn,
        )

    @classmethod
    def build_test_connection_error_message(  # noqa: PLR0913
        cls,
        data_asset_name: str,
        account_name: str,
        container: str,
        name_starts_with: str = "",
        delimiter: str = "/",
        recursive_file_discovery: bool = False,
    ) -> str:
        """Builds helpful error message for reporting issues when linking named DataAsset to Microsoft Azure Blob Storage.

        Args:
            data_asset_name: The name of the DataAsset using this "AzureBlobStorageDataConnector" instance
            account_name: account name for Microsoft Azure Blob Storage
            container: container name for Microsoft Azure Blob Storage
            name_starts_with: Microsoft Azure Blob Storage prefix
            delimiter: Microsoft Azure Blob Storage delimiter
            recursive_file_discovery: Flag to indicate if files should be searched recursively from subfolders

        Returns:
            Customized error message
        """  # noqa: E501
        test_connection_error_message_template: str = 'No file belonging to account "{account_name}" in container "{container}" with prefix "{name_starts_with}" and recursive file discovery set to "{recursive_file_discovery}" found using delimiter "{delimiter}" for DataAsset "{data_asset_name}".'  # noqa: E501
        return test_connection_error_message_template.format(
            **{
                "data_asset_name": data_asset_name,
                "account_name": account_name,
                "container": container,
                "name_starts_with": name_starts_with,
                "delimiter": delimiter,
                "recursive_file_discovery": recursive_file_discovery,
            }
        )

    @override
    def build_batch_spec(self, batch_definition: LegacyBatchDefinition) -> AzureBatchSpec:
        """
        Build BatchSpec from batch_definition by calling DataConnector's build_batch_spec function.

        Args:
            batch_definition (LegacyBatchDefinition): to be used to build batch_spec

        Returns:
            BatchSpec built from batch_definition
        """
        batch_spec: PathBatchSpec = super().build_batch_spec(batch_definition=batch_definition)
        return AzureBatchSpec(batch_spec)

    # Interface Method
    @override
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
    @override
    def _get_full_file_path(self, path: str) -> str:
        if self._file_path_template_map_fn is None:
            raise ValueError(  # noqa: TRY003
                f"""Converting file paths to fully-qualified object references for "{self.__class__.__name__}" \
requires "file_path_template_map_fn: Callable" to be set.
"""  # noqa: E501
            )

        template_arguments: dict = {
            "account_name": self._account_name,
            "container": self._container,
            "path": path,
        }

        return self._file_path_template_map_fn(**template_arguments)

    @override
    def _preprocess_batching_regex(self, regex: re.Pattern) -> re.Pattern:
        regex = re.compile(f"{re.escape(self._sanitized_prefix)}{regex.pattern}")
        return super()._preprocess_batching_regex(regex=regex)


def sanitize_prefix(text: str) -> str:
    """
    Takes in a given user-prefix and cleans it to work with file-system traversal methods
    (i.e. add '/' to the end of a string meant to represent a directory)
    """
    _, ext = os.path.splitext(text)  # noqa: PTH122
    if ext:
        # Provided prefix is a filename so no adjustment is necessary
        return text

    # Provided prefix is a directory (so we want to ensure we append it with '/')
    return os.path.join(text, "")  # noqa: PTH118


def list_azure_keys(
    azure_client: azure.BlobServiceClient,
    query_options: dict,
    recursive: bool = False,
) -> List[str]:
    """
    Utilizes the Azure Blob Storage connection object to retrieve blob names based on user-provided criteria.

    For InferredAssetAzureDataConnector, we take container and name_starts_with and search for files using RegEx at and below the level
    specified by those parameters. However, for ConfiguredAssetAzureDataConnector, we take container and name_starts_with and
    search for files using RegEx only at the level specified by that bucket and prefix.

    This restriction for the ConfiguredAssetAzureDataConnector is needed, because paths on Azure are comprised not only the leaf file name
    but the full path that includes both the prefix and the file name.  Otherwise, in the situations where multiple data assets
    share levels of a directory tree, matching files to data assets will not be possible, due to the path ambiguity.

    Args:
        azure_client (BlobServiceClient): Azure connnection object responsible for accessing container
        query_options (dict): Azure query attributes ("container", "name_starts_with", "delimiter")
        recursive (bool): True for InferredAssetAzureDataConnector and False for ConfiguredAssetAzureDataConnector (see above)

    Returns:
        List of keys representing Azure file paths (as filtered by the query_options dict)
    """  # noqa: E501
    container: str = query_options["container"]
    container_client: azure.ContainerClient = azure_client.get_container_client(container=container)

    path_list: List[str] = []

    def _walk_blob_hierarchy(name_starts_with: str) -> None:
        for item in container_client.walk_blobs(name_starts_with=name_starts_with):
            if isinstance(item, azure.BlobPrefix):
                if recursive:
                    _walk_blob_hierarchy(name_starts_with=item.name)

            else:
                path_list.append(item.name)

    name_starts_with: str = query_options["name_starts_with"]
    _walk_blob_hierarchy(name_starts_with)

    return path_list
