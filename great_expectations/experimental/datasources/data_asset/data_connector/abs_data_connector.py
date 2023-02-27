from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Callable, List, Optional

from great_expectations.core.batch_spec import AzureBatchSpec, PathBatchSpec
from great_expectations.datasource.data_connector.util import (
    list_s3_keys,
    sanitize_prefix,
)
from great_expectations.experimental.datasources.data_asset.data_connector import (
    FilePathDataConnector,
)

if TYPE_CHECKING:
    from great_expectations.core.batch import BatchDefinition


logger = logging.getLogger(__name__)


try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    BlobServiceClient = None
    logger.debug(
        "Unable to load BlobServiceClient connection object; install optional Azure Storage Blob dependency for support"
    )


class ABSDataConnector(FilePathDataConnector):
    """Extension of FilePathDataConnector used to connect to Azure Blob Storage (ABS).


    Args:
        datasource_name: The name of the Datasource associated with this DataConnector instance
        data_asset_name: The name of the DataAsset using this DataConnector instance
        container (str): container name for Azure Blob Storage
        batching_regex: A regex pattern for partitioning data references
        name_starts_with (str): Azure Blob Storage prefix
        delimiter (str): Azure Blob Storage delimiter
        azure_options (dict): wrapper object for **kwargs
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters (list): optional list of sorters for sorting data_references
        file_path_template_map_fn: Format function mapping path to fully-qualified resource on network file storage
        # TODO: <Alex>ALEX</Alex>
    """

    def __init__(
        self,
        datasource_name: str,
        data_asset_name: str,
        container: str,
        batching_regex: re.Pattern,
        name_starts_with: str = "",
        delimiter: str = "/",
        azure_options: Optional[dict] = None,
        # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
        # TODO: <Alex>ALEX</Alex>
        # sorters: Optional[list] = None,
        # TODO: <Alex>ALEX</Alex>
        file_path_template_map_fn: Optional[Callable] = None,
    ) -> None:
        self._container = container
        self._name_starts_with = sanitize_prefix(name_starts_with)
        self._delimiter = delimiter

        # TODO: <Alex>ALEX-PUT_SOME_OF_THIS_INTO_ABS_DATASOURCE_WITH_GET_ABS_CLIENT_LIKE_S3</Alex>
        if azure_options is None:
            azure_options = {}

        # Thanks to schema validation, we are guaranteed to have one of `conn_str` or `account_url` to
        # use in authentication (but not both). If the format or content of the provided keys is invalid,
        # the assignment of `self._account_name` and `self._azure` will fail and an error will be raised.
        conn_str: Optional[str] = azure_options.get("conn_str")
        account_url: Optional[str] = azure_options.get("account_url")
        assert bool(conn_str) ^ bool(
            account_url
        ), "You must provide one of `conn_str` or `account_url` to the `azure_options` key in your config (but not both)"

        try:
            if conn_str is not None:
                self._account_name = re.search(  # type: ignore[union-attr]
                    r".*?AccountName=(.+?);.*?", conn_str
                ).group(1)
                self._azure = BlobServiceClient.from_connection_string(**azure_options)
            elif account_url is not None:
                self._account_name = re.search(  # type: ignore[union-attr]
                    r"(?:https?://)?(.+?).blob.core.windows.net", account_url
                ).group(1)
                self._azure = BlobServiceClient(**azure_options)
        except (TypeError, AttributeError):
            raise ImportError(
                "Unable to load Azure BlobServiceClient (it is required for ConfiguredAssetAzureDataConnector). \
                Please ensure that you have provided the appropriate keys to `azure_options` for authentication."
            )

        super().__init__(
            datasource_name=datasource_name,
            data_asset_name=data_asset_name,
            batching_regex=batching_regex,
            # TODO: <Alex>ALEX_INCLUDE_SORTERS_FUNCTIONALITY_UNDER_PYDANTIC-MAKE_SURE_SORTER_CONFIGURATIONS_ARE_VALIDATED</Alex>
            # TODO: <Alex>ALEX</Alex>
            # sorters=sorters,
            # TODO: <Alex>ALEX</Alex>
            file_path_template_map_fn=file_path_template_map_fn,
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
            "Bucket": self._bucket,
            "Prefix": self._prefix,
            "Delimiter": self._delimiter,
            "MaxKeys": self._max_keys,
        }
        path_list: List[str] = [
            key
            for key in list_s3_keys(
                s3=self._s3_client,
                query_options=query_options,
                iterator_dict={},
                recursive=False,
            )
        ]
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
