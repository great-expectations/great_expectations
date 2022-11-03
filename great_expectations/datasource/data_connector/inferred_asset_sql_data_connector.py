from typing import Dict, List, Optional, Union

from great_expectations.datasource.data_connector.configured_asset_sql_data_connector import (
    ConfiguredAssetSqlDataConnector,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_engine.sqlalchemy_dialect import GESqlDialect
from great_expectations.util import deep_filter_properties_iterable

try:
    import sqlalchemy as sa
    from sqlalchemy.engine import Engine
    from sqlalchemy.engine.reflection import Inspector
    from sqlalchemy.exc import OperationalError
except ImportError:
    sa = None
    Engine = None
    Inspector = None
    OperationalError = None


class InferredAssetSqlDataConnector(ConfiguredAssetSqlDataConnector):
    """
    A DataConnector that infers data_asset names by introspecting a SQL database
    """

    def __init__(
        self,
        name: str,
        datasource_name: str,
        execution_engine: Optional[ExecutionEngine] = None,
        data_asset_name_prefix: str = "",
        data_asset_name_suffix: str = "",
        include_schema_name: bool = False,
        splitter_method: Optional[str] = None,
        splitter_kwargs: Optional[dict] = None,
        sampling_method: Optional[str] = None,
        sampling_kwargs: Optional[dict] = None,
        excluded_tables: Optional[list] = None,
        included_tables: Optional[list] = None,
        skip_inapplicable_tables: bool = True,
        introspection_directives: Optional[dict] = None,
        batch_spec_passthrough: Optional[dict] = None,
        id: Optional[str] = None,
    ) -> None:
        """
        InferredAssetDataConnector for connecting to data on a SQL database

        Args:
            name (str): The name of this DataConnector
            datasource_name (str): The name of the Datasource that contains it
            execution_engine (ExecutionEngine): An ExecutionEngine
            data_asset_name_prefix (str): An optional prefix to prepend to inferred data_asset_names
            data_asset_name_suffix (str): An optional suffix to append to inferred data_asset_names
            include_schema_name (bool): Should the data_asset_name include the schema as a prefix?
            splitter_method (str): A method to split the target table into multiple Batches
            splitter_kwargs (dict): Keyword arguments to pass to splitter_method
            sampling_method (str): A method to downsample within a target Batch
            sampling_kwargs (dict): Keyword arguments to pass to sampling_method
            excluded_tables (List): A list of tables to ignore when inferring data asset_names
            included_tables (List): If not None, only include tables in this list when inferring data asset_names
            skip_inapplicable_tables (bool):
                If True, tables that can't be successfully queried using sampling and splitter methods are excluded from inferred data_asset_names.
                If False, the class will throw an error during initialization if any such tables are encountered.
            introspection_directives (Dict): Arguments passed to the introspection method to guide introspection
            batch_spec_passthrough (dict): dictionary with keys that will be added directly to batch_spec
        """

        super().__init__(
            name=name,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
            include_schema_name=include_schema_name,
            splitter_method=splitter_method,
            splitter_kwargs=splitter_kwargs,
            sampling_method=sampling_method,
            sampling_kwargs=sampling_kwargs,
            assets=None,
            batch_spec_passthrough=batch_spec_passthrough,
            id=id,
        )

        self._data_asset_name_prefix = data_asset_name_prefix
        self._data_asset_name_suffix = data_asset_name_suffix

        self._excluded_tables = excluded_tables
        self._included_tables = included_tables
        self._skip_inapplicable_tables = skip_inapplicable_tables

        if introspection_directives is None:
            introspection_directives = {}

        self._introspection_directives = introspection_directives
        # This cache will contain a "config" for each data_asset discovered via introspection.
        # This approach ensures that ConfiguredAssetSqlDataConnector._assets and _introspected_assets_cache store objects of the same "type"
        # Note: We should probably turn them into AssetConfig objects

        self._refresh_introspected_assets_cache()

    @property
    def data_asset_name_prefix(self) -> str:
        return self._data_asset_name_prefix

    @property
    def data_asset_name_suffix(self) -> str:
        return self._data_asset_name_suffix

    def _refresh_data_references_cache(self) -> None:
        self._refresh_introspected_assets_cache()
        super()._refresh_data_references_cache()

    def _refresh_introspected_assets_cache(self) -> None:
        introspected_table_metadata = self._introspect_db(
            **self._introspection_directives
        )

        introspected_assets: dict = {}

        for metadata in introspected_table_metadata:
            if (self._excluded_tables is not None) and (
                f"{metadata['schema_name']}.{metadata['table_name']}"
                in self._excluded_tables
            ):
                continue

            if (self._included_tables is not None) and (
                f"{metadata['schema_name']}.{metadata['table_name']}"
                not in self._included_tables
            ):
                continue

            schema_name: str = metadata["schema_name"]
            table_name: str = metadata["table_name"]

            data_asset_config: dict = deep_filter_properties_iterable(
                properties={
                    "type": metadata["type"],
                    "table_name": table_name,
                    "data_asset_name_prefix": self.data_asset_name_prefix,
                    "data_asset_name_suffix": self.data_asset_name_suffix,
                    "include_schema_name": self.include_schema_name,
                    "schema_name": schema_name,
                    "splitter_method": self.splitter_method,
                    "splitter_kwargs": self.splitter_kwargs,
                    "sampling_method": self.sampling_method,
                    "sampling_kwargs": self.sampling_kwargs,
                },
            )

            data_asset_name: str = self._update_data_asset_name_from_config(
                data_asset_name=table_name, data_asset_config=data_asset_config
            )

            # Attempt to fetch a list of batch_identifiers from the table
            try:
                self._get_batch_identifiers_list_from_data_asset_config(
                    data_asset_name=data_asset_name,
                    data_asset_config=data_asset_config,
                )
            except OperationalError as e:
                # If it doesn't work, then...
                if self._skip_inapplicable_tables:
                    # No harm done. Just don't include this table in the list of assets.
                    continue
                else:
                    # We're being strict. Crash now.
                    raise ValueError(
                        f"Couldn't execute a query against table {metadata['table_name']} in schema {metadata['schema_name']}"
                    ) from e

            # Store an asset config for each introspected data asset.
            introspected_assets[data_asset_name] = data_asset_config
            self.add_data_asset(name=table_name, config=data_asset_config)

    def _introspect_db(  # noqa: C901 - 16
        self,
        schema_name: Union[str, None] = None,
        ignore_information_schemas_and_system_tables: bool = True,
        information_schemas: Optional[List[str]] = None,
        system_tables: Optional[List[str]] = None,
        include_views=True,
    ):
        if information_schemas is None:
            information_schemas = [
                "INFORMATION_SCHEMA",  # snowflake, mssql, mysql, oracle
                "information_schema",  # postgres, redshift, mysql
                "performance_schema",  # mysql
                "sys",  # mysql
                "mysql",  # mysql
            ]

        if system_tables is None:
            system_tables = ["sqlite_master"]  # sqlite

        engine: Engine = self.execution_engine.engine
        inspector: Inspector = sa.inspect(engine)

        selected_schema_name = schema_name

        tables: List[Dict[str, str]] = []
        schema_names: List[str] = inspector.get_schema_names()
        for schema_name in schema_names:
            if (
                ignore_information_schemas_and_system_tables
                and schema_name in information_schemas
            ):
                continue

            if selected_schema_name is not None and schema_name != selected_schema_name:
                continue

            table_names: List[str] = inspector.get_table_names(schema=schema_name)
            for table_name in table_names:
                if ignore_information_schemas_and_system_tables and (
                    table_name in system_tables
                ):
                    continue

                tables.append(
                    {
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "type": "table",
                    }
                )

            # Note Abe 20201112: This logic is currently untested.
            if include_views:
                # Note: this is not implemented for bigquery
                try:
                    view_names = inspector.get_view_names(schema=schema_name)
                except NotImplementedError:
                    # Not implemented by Athena dialect
                    pass
                else:
                    for view_name in view_names:
                        if ignore_information_schemas_and_system_tables and (
                            view_name in system_tables
                        ):
                            continue

                        tables.append(
                            {
                                "schema_name": schema_name,
                                "table_name": view_name,
                                "type": "view",
                            }
                        )

        # SQLAlchemy's introspection does not list "external tables" in Redshift Spectrum (tables whose data is stored on S3).
        # The following code fetches the names of external schemas and tables from a special table
        # 'svv_external_tables'.
        try:
            if engine.dialect.name.lower() == GESqlDialect.REDSHIFT:
                # noinspection SqlDialectInspection,SqlNoDataSourceInspection
                result = engine.execute(
                    "select schemaname, tablename from svv_external_tables"
                ).fetchall()
                for row in result:
                    tables.append(
                        {
                            "schema_name": row[0],
                            "table_name": row[1],
                            "type": "table",
                        }
                    )
        except Exception as e:
            # Our testing shows that 'svv_external_tables' table is present in all Redshift clusters. This means that this
            # exception is highly unlikely to fire.
            if "UndefinedTable" not in str(e):
                raise e

        return tables
