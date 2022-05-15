import logging
from typing import Any, Dict, Iterator, List, Optional, Tuple

from great_expectations.datasource.data_connector.configured_asset_glue_catalog_data_connector import (
    ConfiguredAssetGlueCatalogDataConnector,
)
from great_expectations.exceptions import DataConnectorError
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)


class InferredAssetGlueCatalogDataConnector(ConfiguredAssetGlueCatalogDataConnector):
    """
    The InferredAssetGlueCatalogDataConnector is one of two classes (ConfiguredAssetGlueCatalogDataConnector being the
    other one) designed for connecting to data through AWS Glue Data Catalog.

    It connects to assets (database and tables) inferred from AWS Glue Data Catalog.

    InferredAssetGlueCatalogDataConnector that operates on AWS Glue Data Catalog and determines
    the data_asset_name implicitly (e.g., by listing all database and table names from Glue Data Catalog)
    """

    def __init__(
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
    ):
        """
        GlueCatalogDataConnector for connecting to AWS Glue Data Catalog.

        Args:
            name (str): Required name for data_connector
            datasource_name (str): Required name for datasource
            execution_engine (ExecutionEngine): Optional reference to ExecutionEngine
            catalog_id (str): Optional catalog ID from which to retrieve databases. If none is provided, the AWS account ID is used by default.
            data_asset_name_prefix (str): An optional prefix to prepend to inferred data_asset_names
            data_asset_name_suffix (str): An optional suffix to append to inferred data_asset_names
            excluded_tables (List): A list of tables ([database].[table]) to ignore when inferring data asset_names
            included_tables (List): If not None, only include tables ([database].[table]) in this list when inferring data asset_names
            glue_introspection_directives (Dict): Arguments passed to the introspection method to guide introspection
            boto3_options (dict): optional boto3 options
            batch_spec_passthrough (dict): dictionary with keys that will be added directly to batch_spec
        """
        logger.warning(
            "Warning: great_expectations.datasource.data_connector.InferredAssetGlueCatalogDataConnector is "
            "experimental. Methods, APIs, and core behavior may change in the future."
        )
        self._catalog_id = catalog_id
        self._data_asset_name_prefix = data_asset_name_prefix
        self._data_asset_name_suffix = data_asset_name_suffix
        self._excluded_tables = excluded_tables
        self._included_tables = included_tables
        self._glue_introspection_directives = glue_introspection_directives or {}

        super().__init__(
            name=name,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
            assets=None,
            batch_spec_passthrough=batch_spec_passthrough,
            boto3_options=boto3_options,
        )

        self._introspected_assets_cache: dict = {}
        self._refresh_introspected_assets_cache(
            self._data_asset_name_prefix,
            self._data_asset_name_suffix,
            self._excluded_tables,
            self._included_tables,
        )

    @property
    def assets(self) -> Dict[str, Any]:
        return self._introspected_assets_cache

    def _refresh_data_references_cache(self) -> None:
        self._refresh_introspected_assets_cache(
            self._data_asset_name_prefix,
            self._data_asset_name_suffix,
            self._excluded_tables,
            self._included_tables,
        )
        super()._refresh_data_references_cache()

    def _refresh_introspected_assets_cache(
        self,
        data_asset_name_prefix: str = None,
        data_asset_name_suffix: str = None,
        excluded_tables: List = None,
        included_tables: List = None,
    ) -> None:
        data_asset_name_prefix = data_asset_name_prefix or ""
        data_asset_name_suffix = data_asset_name_suffix or ""
        introspected_table_metadata = self._introspect_catalog(
            **self._glue_introspection_directives
        )
        for metadata in introspected_table_metadata:
            if (excluded_tables is not None) and (
                f"{metadata['database_name']}.{metadata['table_name']}"
                in excluded_tables
            ):
                continue

            if (included_tables is not None) and (
                f"{metadata['database_name']}.{metadata['table_name']}"
                not in included_tables
            ):
                continue

            data_asset_name = (
                data_asset_name_prefix
                + metadata["database_name"]
                + "."
                + metadata["table_name"]
                + data_asset_name_suffix
            )

            data_asset_config = {
                "database_name": metadata["database_name"],
                "table_name": metadata["table_name"],
                "type": metadata["type"],
            }

            # Attempt to fetch a list of batch_identifiers from the table
            self._get_batch_identifiers_list_from_data_asset_config(
                data_asset_name,
                data_asset_config,
            )

            # Store an asset config for each introspected data asset.
            self._introspected_assets_cache[data_asset_name] = data_asset_config

    def _get_glue_paginator_kwargs(self) -> dict:
        return {"CatalogId": self._catalog_id} if self._catalog_id else {}

    def _get_databases(self) -> Iterator[str]:
        paginator = self._glue.get_paginator("get_databases")
        iterator = paginator.paginate(**self._get_glue_paginator_kwargs())
        for page in iterator:
            for db in page["DatabaseList"]:
                yield db["Name"]

    def _get_tables(self, database: str = None) -> Iterator[Tuple[str, str]]:
        if database:
            databases: List[str] = [database]
        else:
            databases = list(self._get_databases())

        paginator = self._glue.get_paginator("get_tables")
        paginator_kwargs = self._get_glue_paginator_kwargs()
        for db in databases:
            paginator_kwargs["DatabaseName"] = db
            iterator = paginator.paginate(**paginator_kwargs)
            try:
                for page in iterator:
                    for tb in page["TableList"]:
                        database_name = tb["DatabaseName"]
                        table_name = tb["Name"]
                        yield database_name, table_name
            except self._glue.exceptions.EntityNotFoundException:
                raise DataConnectorError(
                    f"InferredAssetGlueCatalogDataConnector could not find a database with name: {db}."
                )

    def _introspect_catalog(self, database_name: str = None) -> List[Dict[str, str]]:
        tables: List[Dict[str, str]] = []
        for db_name, table_name in self._get_tables(database_name):
            tables.append(
                {
                    "database_name": db_name,
                    "table_name": table_name,
                    "type": "table",
                }
            )
        return tables

    def get_available_data_asset_names_and_types(self) -> List[Tuple[str, str]]:
        """
        Return the list of asset names and types known by this DataConnector.

        Returns:
            A list of tuples consisting of available names and types
        """
        return [(asset["table_name"], asset["type"]) for asset in self.assets.values()]
