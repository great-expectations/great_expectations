import logging
import re
from typing import List, Optional

from great_expectations.compatibility import azure
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.batch import BatchDefinition
from great_expectations.core.batch_spec import AzureBatchSpec, PathBatchSpec
from great_expectations.datasource.data_connector.asset import Asset
from great_expectations.datasource.data_connector.configured_asset_file_path_data_connector import (
    ConfiguredAssetFilePathDataConnector,
)
from great_expectations.datasource.data_connector.util import (
    list_azure_keys,
    sanitize_prefix,
)
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


@public_api
class ConfiguredAssetAzureDataConnector(ConfiguredAssetFilePathDataConnector):
    """Extension of ConfiguredAssetFilePathDataConnector used to connect to Azure.

    Being a Configured Asset Data Connector, it requires an explicit list of each Data Asset it can
    connect to. While this allows for fine-grained control over which Data Assets may be accessed,
    it requires more setup.

    Args:
        name (str): required name for DataConnector
        datasource_name (str): required name for datasource
        container (str): container name for Azure Blob Storage
        assets (dict): dict of asset configuration (required for ConfiguredAssetDataConnector)
        execution_engine (ExecutionEngine): optional reference to ExecutionEngine
        default_regex (dict): optional regex configuration for filtering data_references
        sorters (list): optional list of sorters for sorting data_references
        name_starts_with (str): Azure prefix
        delimiter (str): Azure delimiter
        azure_options (dict): wrapper object for **kwargs
        batch_spec_passthrough (dict): dictionary with keys that will be added directly to batch_spec
    """

    def __init__(  # noqa: PLR0913
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
        azure_options: Optional[dict] = None,
        batch_spec_passthrough: Optional[dict] = None,
        id: Optional[str] = None,
    ) -> None:
        logger.debug(f'Constructing ConfiguredAssetAzureDataConnector "{name}".')

        super().__init__(
            name=name,
            id=id,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
            assets=assets,
            default_regex=default_regex,
            sorters=sorters,
            batch_spec_passthrough=batch_spec_passthrough,
        )
        self._container = container
        self._name_starts_with = sanitize_prefix(name_starts_with)
        self._delimiter = delimiter

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
                self._azure = azure.BlobServiceClient.from_connection_string(
                    **azure_options
                )
            elif account_url is not None:
                self._account_name = re.search(  # type: ignore[union-attr]
                    r"(?:https?://)?(.+?).blob.core.windows.net", account_url
                ).group(1)
                self._azure = azure.BlobServiceClient(**azure_options)
        except (TypeError, AttributeError, ModuleNotFoundError):
            raise ImportError(
                "Unable to load Azure BlobServiceClient (it is required for ConfiguredAssetAzureDataConnector). \
                Please ensure that you have provided the appropriate keys to `azure_options` for authentication."
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

    def _get_data_reference_list_for_asset(self, asset: Optional[Asset]) -> List[str]:
        query_options: dict = {
            "container": self._container,
            "name_starts_with": self._name_starts_with,
            "delimiter": self._delimiter,
        }
        if asset is not None:
            if asset.container:
                query_options["container"] = asset.container
            if asset.name_starts_with:
                query_options["name_starts_with"] = asset.name_starts_with
            if asset.delimiter:
                query_options["delimiter"] = asset.delimiter

        path_list: List[str] = list_azure_keys(
            azure_client=self._azure,
            query_options=query_options,
            recursive=False,
        )
        return path_list

    def _get_full_file_path_for_asset(
        self, path: str, asset: Optional[Asset] = None
    ) -> str:
        # asset isn't used in this method.
        # It's only kept for compatibility with parent methods.
        template_arguments: dict = {
            "account_name": self._account_name,
            "container": self._container,
            "path": path,
        }
        return self.resolve_data_reference(template_arguments=template_arguments)
