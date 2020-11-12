import random
from typing import Dict, List

import pandas as pd

from great_expectations.core.batch import (
    BatchDefinition,
    BatchMarkers,
    BatchRequest,
    BatchSpec,
    PartitionDefinition,
    PartitionRequest,
)
from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.asset.asset import Asset
from great_expectations.execution_environment.data_connector.util import batch_definition_matches_batch_request
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.execution_environment.data_connector import ConfiguredAssetSqlDataConnector

try:
    import sqlalchemy as sa
    from sqlalchemy.exc import OperationalError
except ImportError:
    sa = None


class InferredAssetSqlDataConnector(ConfiguredAssetSqlDataConnector):
    def __init__(
        self,
        name: str,
        execution_environment_name: str,
        execution_engine,
        partitioning_directives: Dict={},
        introspection_directives: Dict={},
        # data_context_root_directory=None,
    ):
        self._partitioning_directives = partitioning_directives
        self._introspection_directives = introspection_directives

        super().__init__(
            name=name,
            execution_environment_name=execution_environment_name,
            execution_engine=execution_engine,
            data_assets={},
            # data_context_root_directory=data_context_root_directory,
        )

        # This cache will contain a "config" for each data_asset discovered via introspection.
        # This approach ensures that ConfiguredAssetSqlDataConnector._data_assets and _introspected_data_assets_cache store objects of the same "type"
        # Note: We should probably turn them into AssetConfig objects
        self._introspected_data_assets_cache = {}
        self._refresh_introspected_data_assets_cache(
            **self._partitioning_directives
        )

    @property
    def data_assets(self) -> Dict[str, Asset]:
        return self._introspected_data_assets_cache

    def _refresh_data_references_cache(self):
        self._refresh_introspected_data_assets_cache(
            **self._partitioning_directives
        )

        super()._refresh_data_references_cache()

    def _refresh_introspected_data_assets_cache(
        self,
        data_asset_name_suffix: str=None,
        include_schema_name: bool=False,
        splitter_method: str=None,
        splitter_kwargs: dict=None,
        sampling_method: str=None,
        sampling_kwargs: dict=None,
        excluded_tables: List=None,
        included_tables: List=None,
        skip_inapplicable_tables: bool=True,
    ):
        if data_asset_name_suffix is None:
            data_asset_name_suffix = "__"+self.name

        introspected_table_metadata = self._introspect_db(
            **self._introspection_directives
        )
        for metadata in introspected_table_metadata:
            if (excluded_tables is not None) and (metadata["schema_name"]+"."+metadata["table_name"] in excluded_tables):
                continue

            if (included_tables is not None) and (metadata["schema_name"]+"."+metadata["table_name"] not in included_tables):
                continue

            if include_schema_name:
                data_asset_name = metadata["schema_name"]+"."+metadata["table_name"]+data_asset_name_suffix
            else:
                data_asset_name = metadata["table_name"]+data_asset_name_suffix
            
            data_asset_config = {
                "table_name" : metadata["schema_name"]+"."+metadata["table_name"],
            }
            if not splitter_method is None:
                data_asset_config["splitter_method"] = splitter_method
            if not splitter_kwargs is None:
                data_asset_config["splitter_kwargs"] = splitter_kwargs
            if not sampling_method is None:
                data_asset_config["sampling_method"] = sampling_method
            if not sampling_kwargs is None:
                data_asset_config["sampling_kwargs"] = sampling_kwargs

            if skip_inapplicable_tables:
                # Attempt to fetch a test batch from the table
                print(data_asset_name)
                print(data_asset_config)
                try:
                    self._get_partition_definition_list_from_data_asset_config(
                        data_asset_name,
                        data_asset_config,
                    )
                except OperationalError:
                    continue

            # Store an asset config for each introspected data asset.
            self._introspected_data_assets_cache[data_asset_name] = data_asset_config

    def _introspect_db(
        self,
        schema_name: str=None,
        ignore_information_schemas_and_system_tables: bool=True,
        information_schemas: List[str]= [
            "INFORMATION_SCHEMA",  # snowflake, mssql, mysql, oracle
            "information_schema",  # postgres, redshift, mysql
            "performance_schema",  # mysql
            "sys",  # mysql
            "mysql",  # mysql
        ],
        system_tables: List[str] = ["sqlite_master"],  # sqlite
        include_views = True,
    ):
        engine = self._execution_engine.engine
        inspector = sa.inspect(engine)

        selected_schema_name = schema_name

        tables = []
        for schema_name in inspector.get_schema_names():
            if ignore_information_schemas_and_system_tables and schema_name in information_schemas:
                continue

            if selected_schema_name is not None and schema_name != selected_schema_name:
                continue

            for table_name in inspector.get_table_names(schema=schema_name):

                if (ignore_information_schemas_and_system_tables) and (table_name in system_tables):
                    continue
                
                tables.append({
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "type": "table",
                })

            # Note Abe 20201112: This logic is currently untested.
            if include_views:
                # Note: this is not implemented for bigquery

                for view_name in inspector.get_view_names(schema=schema_name):

                    if (ignore_information_schemas_and_system_tables) and (table_name in system_tables):
                        continue
                    
                    tables.append({
                        "schema_name": schema_name,
                        "table_name": view_name,
                        "type": "view",
                    })
        
        return tables
