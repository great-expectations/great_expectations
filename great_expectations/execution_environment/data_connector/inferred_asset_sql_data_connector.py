from typing import Dict, List

from great_expectations.execution_environment.data_connector.asset.asset import Asset
from great_expectations.execution_environment.data_connector import ConfiguredAssetSqlDataConnector

try:
    import sqlalchemy as sa
    from sqlalchemy.exc import OperationalError
except ImportError:
    sa = None


class InferredAssetSqlDataConnector(ConfiguredAssetSqlDataConnector):
    """A DataConnector that infers data_asset names by introspecting a SQL database

    Args:
        name (str): The name of this DataConnector
        execution_environment_name (str): The name of the ExecutionEnvironment that contains it
        execution_engine (ExecutionEngine): An ExecutionEngine
        data_asset_name_suffix (str): An optional suffix
        include_schema_name (bool):
        splitter_method (str):
        splitter_kwargs (dict):
        sampling_method (str):
        sampling_kwargs (dict):
        excluded_tables (List):
        included_tables (List):
        skip_inapplicable_tables (bool):
        introspection_directives (Dic):
    """
    def __init__(
        self,
        name: str,
        execution_environment_name: str,
        execution_engine,
        data_asset_name_prefix: str="",
        data_asset_name_suffix: str="",
        include_schema_name: bool=False,
        splitter_method: str=None,
        splitter_kwargs: dict=None,
        sampling_method: str=None,
        sampling_kwargs: dict=None,
        excluded_tables: List=None,
        included_tables: List=None,
        skip_inapplicable_tables: bool=True,
        introspection_directives: Dict={},
    ):
        self._data_asset_name_prefix = data_asset_name_prefix
        self._data_asset_name_suffix = data_asset_name_suffix
        self._include_schema_name = include_schema_name
        self._splitter_method = splitter_method
        self._splitter_kwargs = splitter_kwargs
        self._sampling_method = sampling_method
        self._sampling_kwargs = sampling_kwargs
        self._excluded_tables = excluded_tables
        self._included_tables = included_tables
        self._skip_inapplicable_tables = skip_inapplicable_tables

        self._introspection_directives = introspection_directives

        super().__init__(
            name=name,
            execution_environment_name=execution_environment_name,
            execution_engine=execution_engine,
            data_assets={},
        )

        # This cache will contain a "config" for each data_asset discovered via introspection.
        # This approach ensures that ConfiguredAssetSqlDataConnector._data_assets and _introspected_data_assets_cache store objects of the same "type"
        # Note: We should probably turn them into AssetConfig objects
        self._introspected_data_assets_cache = {}
        self._refresh_introspected_data_assets_cache(
            self._data_asset_name_prefix,
            self._data_asset_name_suffix,
            self._include_schema_name,
            self._splitter_method,
            self._splitter_kwargs,
            self._sampling_method,
            self._sampling_kwargs,
            self._excluded_tables,
            self._included_tables,
            self._skip_inapplicable_tables,
        )

    @property
    def data_assets(self) -> Dict[str, Asset]:
        return self._introspected_data_assets_cache

    def _refresh_data_references_cache(self):
        self._refresh_introspected_data_assets_cache(
            self._data_asset_name_prefix,
            self._data_asset_name_suffix,
            self._include_schema_name,
            self._splitter_method,
            self._splitter_kwargs,
            self._sampling_method,
            self._sampling_kwargs,
            self._excluded_tables,
            self._included_tables,
            self._skip_inapplicable_tables,
        )

        super()._refresh_data_references_cache()

    def _refresh_introspected_data_assets_cache(
        self,
        data_asset_name_prefix: str=None,
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
        introspected_table_metadata = self._introspect_db(
            **self._introspection_directives
        )
        for metadata in introspected_table_metadata:
            if (excluded_tables is not None) and (metadata["schema_name"]+"."+metadata["table_name"] in excluded_tables):
                continue

            if (included_tables is not None) and (metadata["schema_name"]+"."+metadata["table_name"] not in included_tables):
                continue

            if include_schema_name:
                data_asset_name = data_asset_name_prefix+metadata["schema_name"]+"."+metadata["table_name"]+data_asset_name_suffix
            else:
                data_asset_name = data_asset_name_prefix+metadata["table_name"]+data_asset_name_suffix
            
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

            # Attempt to fetch a list of partition_definitions from the table
            try:
                self._get_partition_definition_list_from_data_asset_config(
                    data_asset_name,
                    data_asset_config,
                )
            except OperationalError as e:
                # If it doesn't work, then...
                if skip_inapplicable_tables:
                    # No harm done. Just don't include this table in the list of data_assets.
                    continue

                else:
                    #We're being strict. Crash now.
                    raise ValueError(f"Couldn't execute a query against table {metadata['table_name']} in schema {metadata['schema_name']}") from e

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
