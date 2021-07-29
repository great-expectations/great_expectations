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
        prefix: str = "",
        delimiter: str = "/",
        max_keys: int = 1000,
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
        self._prefix = os.path.join(prefix, "")
        self._delimiter = delimiter
        self._max_keys = max_keys

        if azure_options is None:
            azure_options = {}

        try:
            # TODO(cdkini): Implement various methods of instantiation and authentication
            self._azure = BlobServiceClient(**azure_options)
            # self._azure = BlobServiceClient.from_connection_string()
        except (TypeError, AttributeError):
            raise ImportError(
                "Unable to load Azure BlobServiceClient (it is required for ConfiguredAssetAzureDataConnector)."
            )

    def build_batch_spec(self, batch_definition: BatchDefinition) -> AzureBatchSpec:
        batch_spec: PathBatchSpec = super().build_batch_spec(
            batch_definition=batch_definition
        )
        return AzureBatchSpec(batch_spec)

    # FIXME(cdkini): Currently breaks DataConnect.self_check()
    # Let's identify what actually goes in query_options
    def _get_data_reference_list_for_asset(self, asset: Optional[Asset]) -> List[str]:
        query_options: dict = {
            "container": self._container,
            "name_starts_with": self._prefix,
            "delimiter": self._delimiter,
        }
        if asset is not None:
            if asset.bucket:
                query_options["container"] = asset.bucket
            if asset.prefix:
                query_options["name_starts_with"] = asset.prefix
            if asset.delimiter:
                query_options["delimiter"] = asset.delimiter
            # if asset.max_keys:
            #    query_options["MaxKeys"] = asset.max_keys

        path_list: List[str] = [
            key
            for key in list_azure_keys(
                azure=self._azure,
                container=self._container,
                query_options=query_options,
                # iterator_dict={}, # TODO(cdkini): I don't think we need this? Open to check.
                # recursive=False,
            )
        ]
        return path_list

    def _get_full_file_path(
        self,
        path: str,
        data_asset_name: Optional[str] = None,
    ) -> str:
        # data_assert_name isn't used in this method.
        # It's only kept for compatibility with parent methods.
        return f"s3a://{os.path.join(self._azure, path)}"  # TODO(cdkini): Replace with Azure-specific URL
