import logging
from copy import deepcopy
from typing import Any, Dict, List, Optional, cast

from great_expectations.compatibility import aws
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.batch import (
    BatchDefinition,
    BatchRequestBase,
    BatchSpec,
    IDDict,
)
from great_expectations.core.batch_spec import GlueDataCatalogBatchSpec
from great_expectations.datasource.data_connector import DataConnector
from great_expectations.datasource.data_connector.batch_filter import (
    BatchFilter,
    build_batch_filter,
)
from great_expectations.datasource.data_connector.util import (
    batch_definition_matches_batch_request,
)
from great_expectations.exceptions import DataConnectorError
from great_expectations.execution_engine import ExecutionEngine, SparkDFExecutionEngine

logger = logging.getLogger(__name__)


@public_api
class ConfiguredAssetAWSGlueDataCatalogDataConnector(DataConnector):
    """A Configured Asset Data Connector used to connect to data through an AWS Glue Data Catalog.

    Being a Configured Asset Data Connector, it requires an explicit list of each Data Asset it can
    connect to. While this allows for fine-grained control over which Data Assets may be accessed,
    it requires more setup.

    Args:
        name: The name of the Data Connector.
        datasource_name: The name of this Data Connector's Datasource.
        execution_engine: The Execution Engine object to used by this Data Connector to read the data.
        catalog_id: The catalog ID from which to retrieve data. If none is provided, the AWS account
            ID is used by default. Make sure you use the same catalog ID as configured in your spark session.
        partitions: A list of partition keys to be defined for all Data Assets. The partitions defined in Data Asset
            config will override the partitions defined in the connector level.
        assets: A mapping of Data Asset names to their configuration.
        boto3_options: Options passed to the `boto3` library.
        batch_spec_passthrough: Dictionary with keys that will be added directly to the batch spec.
        id: The unique identifier for this Data Connector used when running in cloud mode.
    """

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        datasource_name: str,
        execution_engine: Optional[ExecutionEngine] = None,
        catalog_id: Optional[str] = None,
        partitions: Optional[List[str]] = None,
        assets: Optional[Dict[str, dict]] = None,
        boto3_options: Optional[dict] = None,
        batch_spec_passthrough: Optional[dict] = None,
        id: Optional[str] = None,
    ):
        logger.warning(
            "Warning: great_expectations.datasource.data_connector.ConfiguredAssetAWSGlueDataCatalogDataConnector is "
            "experimental. Methods, APIs, and core behavior may change in the future."
        )
        if execution_engine:
            execution_engine = cast(SparkDFExecutionEngine, execution_engine)

        super().__init__(
            name=name,
            datasource_name=datasource_name,
            execution_engine=execution_engine,  # type: ignore[arg-type]
            batch_spec_passthrough=batch_spec_passthrough,
            id=id,
        )
        if boto3_options is None:
            boto3_options = {}

        try:
            self._glue_client: Any = aws.boto3.client("glue", **boto3_options)
        except (TypeError, AttributeError):
            raise ImportError(
                "Unable to load boto3 (it is required for ConfiguredAssetAWSGlueDataCatalogDataConnector)."
            )

        self._catalog_id = catalog_id
        self._partitions = partitions

        self._assets: Dict[str, dict] = {}
        self._refresh_data_assets_cache(assets=assets)

        self._data_references_cache: Dict[str, List[dict]] = {}

    @property
    def catalog_id(self) -> Optional[str]:
        return self._catalog_id

    @property
    def glue_client(self) -> Any:
        return self._glue_client

    @property
    def assets(self) -> Dict[str, dict]:
        return self._assets

    @property
    def partitions(self) -> Optional[List[str]]:
        return self._partitions

    def build_batch_spec(
        self, batch_definition: BatchDefinition
    ) -> GlueDataCatalogBatchSpec:
        """
        Build BatchSpec from batch_definition by calling DataConnector's build_batch_spec function.

        Args:
            batch_definition (BatchDefinition): to be used to build batch_spec

        Returns:
            BatchSpec built from batch_definition
        """

        data_asset_name: str = batch_definition.data_asset_name
        if (
            data_asset_name in self.assets
            and self.assets[data_asset_name].get("batch_spec_passthrough")
            and isinstance(
                self.assets[data_asset_name].get("batch_spec_passthrough"), dict
            )
        ):
            # batch_spec_passthrough from data_asset
            batch_spec_passthrough = deepcopy(
                self.assets[data_asset_name]["batch_spec_passthrough"]
            )
            batch_definition_batch_spec_passthrough = (
                deepcopy(batch_definition.batch_spec_passthrough) or {}
            )
            # batch_spec_passthrough from Batch Definition supersedes batch_spec_passthrough from data_asset
            batch_spec_passthrough.update(batch_definition_batch_spec_passthrough)
            batch_definition.batch_spec_passthrough = batch_spec_passthrough

        batch_spec: BatchSpec = super().build_batch_spec(
            batch_definition=batch_definition
        )

        return GlueDataCatalogBatchSpec(batch_spec)

    @public_api
    def get_available_data_asset_names(self) -> List[str]:
        """Return the list of asset names known by this DataConnector.

        Returns:
            A list of available names
        """
        return list(self.assets.keys())

    def get_unmatched_data_references(self) -> List[str]:
        """
        Returns the list of data_references unmatched by configuration by looping through items in _data_references_cache
        and returning data_reference that do not have an associated data_asset.

        Returns:
            list of data_references that are not matched by configuration.
        """
        return []

    def get_batch_definition_list_from_batch_request(
        self, batch_request: BatchRequestBase
    ):
        """
        Retrieve batch_definitions that match batch_request

        First retrieves all batch_definitions that match batch_request
            - if batch_request also has a batch_filter, then select batch_definitions that match batch_filter.
            - NOTE : currently glue data connectors do not support sorters.

        Args:
            batch_request (BatchRequestBase): BatchRequestBase (BatchRequest without attribute validation) to process

        Returns:
            A list of BatchDefinition objects that match BatchRequest
        """
        self._validate_batch_request(batch_request=batch_request)

        if len(self._data_references_cache) == 0:
            self._refresh_data_references_cache()

        batch_definition_list: List[BatchDefinition] = []
        try:
            sub_cache = self._get_data_reference_list_from_cache_by_data_asset_name(
                data_asset_name=batch_request.data_asset_name
            )
        except KeyError:
            raise KeyError(
                f"data_asset_name {batch_request.data_asset_name} is not recognized."
            )

        for batch_identifiers in sub_cache:
            batch_definition = BatchDefinition(
                datasource_name=self.datasource_name,
                data_connector_name=self.name,
                data_asset_name=batch_request.data_asset_name,
                batch_identifiers=IDDict(batch_identifiers),
                batch_spec_passthrough=batch_request.batch_spec_passthrough,
            )
            if batch_definition_matches_batch_request(batch_definition, batch_request):
                batch_definition_list.append(batch_definition)

        if batch_request.data_connector_query is not None:
            data_connector_query_dict = batch_request.data_connector_query.copy()
            if (
                batch_request.limit is not None
                and data_connector_query_dict.get("limit") is None
            ):
                data_connector_query_dict["limit"] = batch_request.limit

            batch_filter_obj: BatchFilter = build_batch_filter(
                data_connector_query_dict=data_connector_query_dict
            )
            batch_definition_list = batch_filter_obj.select_from_data_connector_query(
                batch_definition_list=batch_definition_list
            )

        return batch_definition_list

    def add_data_asset(
        self,
        name: str,
        config: dict,
    ) -> None:
        """
        Add data_asset to DataConnector using data_asset name as key, and data_asset config as value.
        """
        if "database_name" not in config:
            raise DataConnectorError(
                message=f"{self.__class__.__name__} ran into an error while initializing Asset names, 'database_name' was not specified"
            )
        if "table_name" not in config:
            raise DataConnectorError(
                message=f"{self.__class__.__name__} ran into an error while initializing Asset names, 'table_name' was not specified"
            )

        if self.partitions:
            # Asset config partitions takes precedence.
            config["partitions"] = config.get("partitions", self.partitions)

        partitions = config.get("partitions")
        if partitions and not isinstance(partitions, list):
            raise DataConnectorError(
                message=f"{self.__class__.__name__} ran into an error while initializing Asset names, 'partitions' must be a list, got {type(partitions)}"
            )

        name = self._update_data_asset_name_from_config(
            data_asset_name=name, data_asset_config=config
        )

        self._assets[name] = config

    def _get_glue_paginator_kwargs(self) -> dict:
        return {"CatalogId": self.catalog_id} if self.catalog_id else {}

    def _get_table_partitions(self, database_name: str, table_name: str) -> List[str]:
        paginator_kwargs = self._get_glue_paginator_kwargs()
        paginator_kwargs["DatabaseName"] = database_name
        paginator_kwargs["Name"] = table_name
        try:
            table = self.glue_client.get_table(**paginator_kwargs)
            return [p["Name"] for p in table["Table"]["PartitionKeys"]]
        except self.glue_client.exceptions.EntityNotFoundException:
            raise DataConnectorError(
                f"ConfiguredAssetAWSGlueDataCatalogDataConnector could not find a table with name: {database_name}.{table_name}"
            )

    def _get_batch_identifiers(
        self, partition_keys: List[str], database_name: str, table_name: str
    ) -> List[dict]:
        batch_identifiers: List[dict] = []
        table_partitions: List[str] = self._get_table_partitions(
            database_name=database_name, table_name=table_name
        )

        paginator_kwargs: dict = self._get_glue_paginator_kwargs()
        paginator_kwargs["DatabaseName"] = database_name
        paginator_kwargs["TableName"] = table_name
        paginator = self.glue_client.get_paginator("get_partitions")
        iterator = paginator.paginate(**paginator_kwargs)
        for page in iterator:
            for partition in page["Partitions"]:
                partition_values = partition["Values"]
                batch_id = dict(zip(table_partitions, partition_values))
                filtered_batch_id = dict(
                    filter(lambda k: k[0] in partition_keys, batch_id.items())
                )

                if filtered_batch_id not in batch_identifiers:
                    batch_identifiers.append(filtered_batch_id)

        return batch_identifiers

    def _get_batch_identifiers_list_from_data_asset_config(
        self, data_asset_config: dict
    ) -> List[dict]:
        table_name: str = data_asset_config["table_name"]
        database_name: str = data_asset_config["database_name"]
        partitions: Optional[list] = data_asset_config.get("partitions")

        batch_identifiers_list: Optional[List[dict]] = None
        if partitions:
            batch_identifiers_list = self._get_batch_identifiers(
                partition_keys=partitions,
                database_name=database_name,
                table_name=table_name,
            )

        return batch_identifiers_list or [{}]

    def _refresh_data_references_cache(self) -> None:
        self._data_references_cache = {}

        for data_asset_name in self.assets:
            data_asset_config: dict = self.assets[data_asset_name]
            batch_identifiers_list: List[
                dict
            ] = self._get_batch_identifiers_list_from_data_asset_config(
                data_asset_config=data_asset_config
            )
            self._data_references_cache[data_asset_name] = batch_identifiers_list

    def _get_data_reference_list_from_cache_by_data_asset_name(
        self, data_asset_name: str
    ) -> List[dict]:
        return self._data_references_cache[data_asset_name]

    def _update_data_asset_name_from_config(
        self, data_asset_name: str, data_asset_config: dict
    ) -> str:
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

    def _map_data_reference_to_batch_definition_list(
        self, data_reference, data_asset_name: Optional[str] = None
    ) -> Optional[List[BatchDefinition]]:
        # Note: data references *are* dictionaries, allowing us to invoke `IDDict(data_reference)`
        return [
            BatchDefinition(
                datasource_name=self.datasource_name,
                data_connector_name=self.name,
                data_asset_name=data_asset_name,  # type: ignore[arg-type]
                batch_identifiers=IDDict(data_reference),
            )
        ]

    def _generate_batch_spec_parameters_from_batch_definition(
        self, batch_definition: BatchDefinition
    ) -> dict:
        """
        Build BatchSpec parameters from batch_definition with the following components:
            1. data_asset_name from batch_definition
            2. batch_identifiers from batch_definition
            3. data_asset from data_connector

        Args:
            batch_definition (BatchDefinition): to be used to build batch_spec

        Returns:
            dict built from batch_definition
        """
        data_asset_name: str = batch_definition.data_asset_name
        data_asset_config: dict = self.assets[data_asset_name]
        return {
            "data_asset_name": data_asset_name,
            "batch_identifiers": batch_definition.batch_identifiers,
            **data_asset_config,
        }

    def _refresh_data_assets_cache(
        self, assets: Optional[Dict[str, dict]] = None
    ) -> None:
        # Clear assets already stored in memory.
        self._assets = {}

        assets = assets or {}
        for data_asset_name, data_asset_config in assets.items():
            self.add_data_asset(name=data_asset_name, config=data_asset_config)
