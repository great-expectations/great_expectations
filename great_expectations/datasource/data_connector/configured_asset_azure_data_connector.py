
import logging
import re
from typing import List, Optional
from great_expectations.core.batch import BatchDefinition
from great_expectations.core.batch_spec import AzureBatchSpec, PathBatchSpec
from great_expectations.datasource.data_connector.asset import Asset
from great_expectations.datasource.data_connector.configured_asset_file_path_data_connector import ConfiguredAssetFilePathDataConnector
from great_expectations.datasource.data_connector.file_path_data_connector import FilePathDataConnector
from great_expectations.datasource.data_connector.util import list_azure_keys
from great_expectations.execution_engine import ExecutionEngine
logger = logging.getLogger(__name__)
try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    BlobServiceClient = None
    logger.debug('Unable to load BlobServiceClient connection object; install optional Azure Storage Blob dependency for support')

class ConfiguredAssetAzureDataConnector(ConfiguredAssetFilePathDataConnector):
    '\n    Extension of ConfiguredAssetFilePathDataConnector used to connect to Azure\n\n    DataConnectors produce identifying information, called "batch_spec" that ExecutionEngines\n    can use to get individual batches of data. They add flexibility in how to obtain data\n    such as with time-based partitioning, splitting and sampling, or other techniques appropriate\n    for obtaining batches of data.\n\n    The ConfiguredAssetAzureDataConnector is one of two classes (InferredAssetAzureDataConnector being the\n    other one) designed for connecting to data on Azure.\n\n    A ConfiguredAssetAzureDataConnector requires an explicit specification of each DataAsset you want to connect to.\n    This allows more fine-tuning, but also requires more setup. Please note that in order to maintain consistency\n    with Azure\'s official SDK, we utilize terms like "container" and "name_starts_with".\n\n    As much of the interaction with the SDK is done through a BlobServiceClient, please refer to the official\n    docs if a greater understanding of the supported authentication methods and general functionality is desired.\n    Source: https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobserviceclient?view=azure-python\n    '

    def __init__(self, name: str, datasource_name: str, container: str, assets: dict, execution_engine: Optional[ExecutionEngine]=None, default_regex: Optional[dict]=None, sorters: Optional[list]=None, name_starts_with: str='', delimiter: str='/', azure_options: Optional[dict]=None, batch_spec_passthrough: Optional[dict]=None) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        ConfiguredAssetDataConnector for connecting to Azure.\n\n        Args:\n            name (str): required name for DataConnector\n            datasource_name (str): required name for datasource\n            container (str): container name for Azure Blob Storage\n            assets (dict): dict of asset configuration (required for ConfiguredAssetDataConnector)\n            execution_engine (ExecutionEngine): optional reference to ExecutionEngine\n            default_regex (dict): optional regex configuration for filtering data_references\n            sorters (list): optional list of sorters for sorting data_references\n            name_starts_with (str): Azure prefix\n            delimiter (str): Azure delimiter\n            azure_options (dict): wrapper object for **kwargs\n            batch_spec_passthrough (dict): dictionary with keys that will be added directly to batch_spec\n        '
        logger.debug(f'Constructing ConfiguredAssetAzureDataConnector "{name}".')
        super().__init__(name=name, datasource_name=datasource_name, execution_engine=execution_engine, assets=assets, default_regex=default_regex, sorters=sorters, batch_spec_passthrough=batch_spec_passthrough)
        self._container = container
        self._name_starts_with = FilePathDataConnector.sanitize_prefix(name_starts_with)
        self._delimiter = delimiter
        if (azure_options is None):
            azure_options = {}
        conn_str: Optional[str] = azure_options.get('conn_str')
        account_url: Optional[str] = azure_options.get('account_url')
        assert (bool(conn_str) ^ bool(account_url)), 'You must provide one of `conn_str` or `account_url` to the `azure_options` key in your config (but not both)'
        try:
            if (conn_str is not None):
                self._account_name = re.search('.*?AccountName=(.+?);.*?', conn_str).group(1)
                self._azure = BlobServiceClient.from_connection_string(**azure_options)
            elif (account_url is not None):
                self._account_name = re.search('(?:https?://)?(.+?).blob.core.windows.net', account_url).group(1)
                self._azure = BlobServiceClient(**azure_options)
        except (TypeError, AttributeError):
            raise ImportError('Unable to load Azure BlobServiceClient (it is required for ConfiguredAssetAzureDataConnector).                 Please ensure that you have provided the appropriate keys to `azure_options` for authentication.')

    def build_batch_spec(self, batch_definition: BatchDefinition) -> AzureBatchSpec:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        "\n        Build BatchSpec from batch_definition by calling DataConnector's build_batch_spec function.\n\n        Args:\n            batch_definition (BatchDefinition): to be used to build batch_spec\n\n        Returns:\n            BatchSpec built from batch_definition\n        "
        batch_spec: PathBatchSpec = super().build_batch_spec(batch_definition=batch_definition)
        return AzureBatchSpec(batch_spec)

    def _get_data_reference_list_for_asset(self, asset: Optional[Asset]) -> List[str]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        query_options: dict = {'container': self._container, 'name_starts_with': self._name_starts_with, 'delimiter': self._delimiter}
        if (asset is not None):
            if asset.container:
                query_options['container'] = asset.container
            if asset.name_starts_with:
                query_options['name_starts_with'] = asset.name_starts_with
            if asset.delimiter:
                query_options['delimiter'] = asset.delimiter
        path_list: List[str] = list_azure_keys(azure=self._azure, query_options=query_options, recursive=False)
        return path_list

    def _get_full_file_path_for_asset(self, path: str, asset: Optional[Asset]=None) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        template_arguments: dict = {'account_name': self._account_name, 'container': self._container, 'path': path}
        return self.execution_engine.resolve_data_reference(data_connector_name=self.__class__.__name__, template_arguments=template_arguments)
