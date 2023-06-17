import logging
from typing import Iterator, List, Optional

from great_expectations.core._docs_decorators import public_api
from great_expectations.datasource.data_connector.configured_asset_aws_glue_data_catalog_data_connector import (
    ConfiguredAssetAWSGlueDataCatalogDataConnector,
)
from great_expectations.exceptions import DataConnectorError
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


@public_api
class InferredAssetAWSGlueDataCatalogDataConnector(
    ConfiguredAssetAWSGlueDataCatalogDataConnector
):
    """An Inferred Asset Data Connector used to connect to data through an AWS Glue Data Catalog.

    This Data Connector operates on AWS Glue Data Catalog and determines the Data Asset name
    implicitly, by listing all databases, tables, and partitions from AWS Glue Data Catalog.

    Args:
        name: The name of the Data Connector.
        datasource_name: The name of this Data Connector's Datasource.
        execution_engine: The Execution Engine object to used by this Data Connector to read the data.
        catalog_id: The catalog ID from which to retrieve data. If none is provided, the AWS account
            ID is used by default. Make sure you use the same catalog ID as configured in your spark session.
        data_asset_name_prefix: A prefix to prepend to all names of Data Assets inferred by this Data Connector.
        data_asset_name_suffix: A suffix to append to all names of Data Asset inferred by this Data Connector.
        excluded_tables: A list of tables, in the form `([database].[table])`, to ignore when inferring Data Asset
            names.
        included_tables: A list of tables, in the form `([database].[table])`, to include when inferring Data Asset
            names. When provided, only Data Assets matching this list will be inferred.
        glue_introspection_directives: Arguments passed to the introspection method. Currently, the only available
            directive is
            `database` which filters to assets only in this database.
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
        data_asset_name_prefix: str = "",
        data_asset_name_suffix: str = "",
        excluded_tables: Optional[list] = None,
        included_tables: Optional[list] = None,
        glue_introspection_directives: Optional[dict] = None,
        boto3_options: Optional[dict] = None,
        batch_spec_passthrough: Optional[dict] = None,
        id: Optional[str] = None,
    ):
        logger.warning(
            "Warning: great_expectations.datasource.data_connector.InferredAssetAWSGlueDataCatalogDataConnector is "
            "experimental. Methods, APIs, and core behavior may change in the future."
        )
        super().__init__(
            name=name,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
            catalog_id=catalog_id,
            assets=None,
            batch_spec_passthrough=batch_spec_passthrough,
            boto3_options=boto3_options,
            id=id,
        )

        self._data_asset_name_prefix = data_asset_name_prefix
        self._data_asset_name_suffix = data_asset_name_suffix
        self._excluded_tables = excluded_tables
        self._included_tables = included_tables
        self._glue_introspection_directives = glue_introspection_directives or {}

        self._refresh_introspected_assets_cache()

    @property
    def data_asset_name_prefix(self) -> str:
        return self._data_asset_name_prefix

    @property
    def data_asset_name_suffix(self) -> str:
        return self._data_asset_name_suffix

    @property
    def excluded_tables(self) -> Optional[list]:
        return self._excluded_tables

    @property
    def included_tables(self) -> Optional[list]:
        return self._included_tables

    @property
    def glue_introspection_directives(self) -> dict:
        return self._glue_introspection_directives

    def _refresh_data_references_cache(self) -> None:
        self._refresh_introspected_assets_cache()
        super()._refresh_data_references_cache()

    def _refresh_introspected_assets_cache(self) -> None:
        introspected_table_metadata = self._introspect_catalog(
            **self.glue_introspection_directives
        )

        introspected_assets: dict = {}
        for metadata in introspected_table_metadata:
            # For the inferred glue connector, the data asset name is database.table
            data_asset_name = f"{metadata['database_name']}.{metadata['table_name']}"

            if (self.excluded_tables is not None) and (
                data_asset_name in self.excluded_tables
            ):
                continue

            if (self.included_tables is not None) and (
                data_asset_name not in self.included_tables
            ):
                continue

            data_asset_config: dict = {
                "database_name": metadata["database_name"],
                "table_name": metadata["table_name"],
                "partitions": metadata["partitions"],
                "data_asset_name_prefix": self.data_asset_name_prefix,
                "data_asset_name_suffix": self.data_asset_name_suffix,
            }

            introspected_assets[data_asset_name] = data_asset_config

        self._refresh_data_assets_cache(assets=introspected_assets)

    def _get_databases(self) -> Iterator[str]:
        paginator = self.glue_client.get_paginator("get_databases")
        iterator = paginator.paginate(**self._get_glue_paginator_kwargs())
        for page in iterator:
            for db in page["DatabaseList"]:
                yield db["Name"]

    def _introspect_catalog(self, database_name: Optional[str] = None) -> List[dict]:
        paginator = self.glue_client.get_paginator("get_tables")
        paginator_kwargs = self._get_glue_paginator_kwargs()

        databases: List[str] = (
            [database_name] if database_name else list(self._get_databases())
        )
        tables: List[dict] = []
        for db in databases:
            paginator_kwargs["DatabaseName"] = db
            iterator = paginator.paginate(**paginator_kwargs)
            try:
                for page in iterator:
                    for tb in page["TableList"]:
                        tables.append(
                            {
                                "database_name": tb["DatabaseName"],
                                "table_name": tb["Name"],
                                "partitions": [p["Name"] for p in tb["PartitionKeys"]],
                            }
                        )
            except self.glue_client.exceptions.EntityNotFoundException:
                raise DataConnectorError(
                    f"InferredAssetAWSGlueDataCatalogDataConnector could not find a database with name: {db}."
                )
        return tables
