import logging
from typing import Dict, Optional, cast

from great_expectations.core.batch import BatchDefinition, BatchSpec
from great_expectations.core.batch_spec import GlueCatalogBatchSpec
from great_expectations.datasource.data_connector.configured_asset_sql_data_connector import (
    ConfiguredAssetSqlDataConnector,
)
from great_expectations.execution_engine import ExecutionEngine, SparkDFExecutionEngine

try:
    import boto3
except ImportError:
    boto3 = None

logger = logging.getLogger(__name__)


class ConfiguredAssetGlueCatalogDataConnector(ConfiguredAssetSqlDataConnector):
    """
    Extension of ConfiguredAssetSqlDataConnector used to connect to AWS Glue Data Catalog

    The ConfiguredAssetGlueCatalogDataConnector is one of two classes (InferredAssetGlueCatalogDataConnector
    being the other one) designed for connecting to data through AWS Glue Data Catalog.

    A ConfiguredAssetGlueCatalogDataConnector requires an explicit listing of each DataAsset you want to
    connect to. This allows more fine-tuning, but also requires more setup. You will need define to the
    database and table names.
    """

    def __init__(
        self,
        name: str,
        datasource_name: str,
        execution_engine: Optional[ExecutionEngine] = None,
        assets: Optional[Dict[str, dict]] = None,
        boto3_options: Optional[dict] = None,
        batch_spec_passthrough: Optional[dict] = None,
    ):
        """
        A DataConnector that requires explicit listing of AWS Glue Catalog tables you want to connect to.

        Args:
            name (str): The name of this DataConnector
            datasource_name (str): Name of datasource that this DataConnector is connected to
            execution_engine (ExecutionEngine): Execution Engine object to actually read the data
            assets (dict): dict of asset configuration
            boto3_options (dict): optional boto3 options
            batch_spec_passthrough (dict): dictionary with keys that will be added directly to batch_spec
        """
        logger.warning(
            "Warning: great_expectations.datasource.data_connector.ConfiguredAssetGlueCatalogDataConnector is "
            "experimental. Methods, APIs, and core behavior may change in the future."
        )
        super().__init__(
            name=name,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
            assets=assets,
            batch_spec_passthrough=batch_spec_passthrough,
        )

        if boto3_options is None:
            boto3_options = {}

        try:
            self._glue = boto3.client("glue", **boto3_options)
        except (TypeError, AttributeError):
            raise ImportError(
                "Unable to load boto3 (it is required for ConfiguredAssetGlueCatalogDataConnector)."
            )

    @property
    def execution_engine(self) -> SparkDFExecutionEngine:
        return cast(SparkDFExecutionEngine, self._execution_engine)

    def _update_data_asset_name_from_config(
        self, data_asset_name: str, data_asset_config: Optional[dict]
    ) -> str:
        if data_asset_config is None:
            data_asset_config = {}

        data_asset_name_prefix: str = data_asset_config.get(
            "data_asset_name_prefix", ""
        )
        data_asset_name_suffix: str = data_asset_config.get(
            "data_asset_name_suffix", ""
        )

        data_asset_name = (
            f"{data_asset_name_prefix}{data_asset_name}{data_asset_name_suffix}"
        )
        return data_asset_name

    def _get_batch_identifiers_list_from_data_asset_config(
        self,
        data_asset_name,
        data_asset_config,
    ):
        return [{}]

    def build_batch_spec(
        self, batch_definition: BatchDefinition
    ) -> GlueCatalogBatchSpec:
        """
        Build BatchSpec from batch_definition by calling DataConnector's build_batch_spec function.

        Args:
            batch_definition (BatchDefinition): to be used to build batch_spec

        Returns:
            BatchSpec built from batch_definition
        """
        batch_spec: BatchSpec = super().build_batch_spec(
            batch_definition=batch_definition
        )
        return GlueCatalogBatchSpec(batch_spec)

    def _get_table_name_from_batch_definition(
        self, batch_definition: BatchDefinition
    ) -> str:
        """
            Helper method called by _get_batch_identifiers_list_from_data_asset_config() to parse table_name from data_asset_name in cases
            where database_name is included.

            data_asset_name in those cases are [database_name].[table_name].

        function will split data_asset_name on [database_name]. and return the resulting table_name.
        """
        data_asset_dict: dict = self.assets[batch_definition.data_asset_name]
        if "table_name" in data_asset_dict:
            return data_asset_dict["table_name"]

        table_name: str = batch_definition.data_asset_name
        if "database_name" in data_asset_dict:
            database_name_str: str = data_asset_dict["database_name"]
            if database_name_str in table_name:
                table_name = table_name.split(f"{database_name_str}.")[1]

        return table_name
