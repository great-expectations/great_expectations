import logging
import os
from typing import List, Optional

try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    azure.storage.blob = None

from great_expectations.core.batch import BatchDefinition
from great_expectations.core.batch_spec import AzureBatchSpec, PathBatchSpec
from great_expectations.datasource.data_connector import (
    ConfiguredAssetFilePathDataConnector,
)
from great_expectations.datasource.data_connector.asset import Asset
from great_expectations.datasource.data_connector.util import list_azure_keys
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


class ConfiguredAssetAzureDataConnector(ConfiguredAssetFilePathDataConnector):
    def __init__(
        self,
        name: str,
        datasource_name: str,
        container: str,
        assets: dict,
        execution_engine: Optional[ExecutionEngine] = None,
        default_regex: Optional[dict] = None,
        sorters: Optional[list] = None,
        name_starts_with: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,  # TODO(cdkini): Do we need to address this in Azure?
        azure_options: Optional[dict] = None,
        batch_spec_passthrough: Optional[dict] = None,
    ):
        logger.debug(f'Constructing ConfiguredAssetAzureDataConnector "{name}".')

        super().__init__(
            name=name,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
            assets=assets,
            default_regex=default_regex,
            sorters=sorters,
            batch_spec_passthrough=batch_spec_passthrough,
        )
        self._container = container
        self._name_starts_with = os.path.join(name_starts_with, "")
        self._delimiter = delimiter
        self._max_keys = max_keys

        if azure_options is None:
            azure_options = {}

        try:
            if "conn_str" in azure_options:
                self._azure = BlobServiceClient.from_connection_string(**azure_options)
            else:
                self._azure = BlobServiceClient(**azure_options)
        except (TypeError, AttributeError):
            raise ImportError(
                "Unable to load Azure BlobServiceClient (it is required for ConfiguredAssetAzureDataConnector)."
            )

    def build_batch_spec(self, batch_definition: BatchDefinition) -> AzureBatchSpec:
        batch_spec: PathBatchSpec = super().build_batch_spec(
            batch_definition=batch_definition
        )
        return AzureBatchSpec(batch_spec)

    def _get_data_reference_list_for_asset(self, asset: Optional[Asset]) -> List[str]:
        query_options: dict = {
            "container": self._container,
            "name_starts_with": self._name_starts_with,
            "delimiter": self._delimiter,
        }
        if asset is not None:
            if asset.bucket:
                query_options["container"] = asset.bucket
            if asset.prefix:
                query_options["name_starts_with"] = asset.prefix
            if asset.delimiter:
                query_options["delimiter"] = asset.delimiter

        path_list: List[str] = list_azure_keys(
            azure=self._azure,
            query_options=query_options,
            recursive=False,
        )
        return path_list

    def _get_full_file_path(
        self,
        path: str,
        data_asset_name: Optional[str] = None,
    ) -> str:
        # data_asset_name isn't used in this method.
        # It's only kept for compatibility with parent methods.
        # http://<storage_account_name>.blob.core.windows.net/<container_name>/<blob_name>
        # return f"http://{storage_account_name}.blob.core.windows.net/{self._container}/{path}"
        return f"s3a://{os.path.join(self._azure, path)}"  # TODO(cdkini): Replace with Azure-specific URL
