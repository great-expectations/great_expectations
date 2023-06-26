from typing import Dict, List, Optional, Union

from great_expectations.compatibility import sqlalchemy
from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.core._docs_decorators import public_api
from great_expectations.datasource.data_connector.configured_asset_sql_data_connector import (
    ConfiguredAssetSqlDataConnector,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
from great_expectations.util import deep_filter_properties_iterable


@public_api
class InferredAssetSqlDataConnector(ConfiguredAssetSqlDataConnector):
    """An Inferred Asset Data Connector used to connect to an SQL database.

    This Data Connector determines Data Asset names by introspecting the database schema.

    Args:
        name: The name of the Data Connector.
        datasource_name: The name of this Data Connector's Datasource.
        execution_engine: The Execution Engine object to used by this Data Connector to read the data.
        data_asset_name_prefix: A prefix to prepend to all names of Data Assets inferred by this Data Connector.
        data_asset_name_suffix: A suffix to append to all names of Data Asset inferred by this Data Connector.
        include_schema_name: If True the Data Asset name  will include the schema as a prefix.
        splitter_method: A method to use to split the target table into multiple Batches.
        splitter_kwargs: Keyword arguments to pass to the splitter method.
        sampling_method: A method to use to downsample within a target Batch.
        sampling_kwargs: Keyword arguments to pass to sampling method.
        excluded_tables: A list of tables to ignore when inferring Data Asset names.
        included_tables: A list of tables to include when inferring Data Asset names. When provided, only Data Assets
            matching this list will be inferred.
        skip_inapplicable_tables: If True, tables that can't be successfully queried using sampling and splitter methods
            are excluded from inferred data_asset_names. If False, the class will throw an error during initialization
            if any such tables are encountered.
        introspection_directives: Arguments passed to the introspection method to guide introspection. These may be,
            `schema_name`, which filters to a specific schema,
            `ignore_information_schemas_and_system_tables`, which defaults to True,
            `information_schemas`, a list of schemas to consider as informational,
            `system_tables`, a list of tables to consider system tables,
            and `include_views` which defaults to True.
        batch_spec_passthrough: Dictionary with keys that will be added directly to the batch spec.
        id: The unique identifier for this Data Connector used when running in cloud mode.
    """

    def __init__(  # noqa: PLR0913
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
            except sqlalchemy.OperationalError as e:
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

    def _introspect_db(  # noqa: C901, PLR0912, PLR0913
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

        engine: sqlalchemy.Engine = self.execution_engine.engine
        inspector: sqlalchemy.Inspector = sa.inspect(engine)

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
            if engine.dialect.name.lower() == GXSqlDialect.REDSHIFT:
                # noinspection SqlDialectInspection,SqlNoDataSourceInspection
                result = self.execution_engine.execute_query(
                    sa.text("select schemaname, tablename from svv_external_tables")
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
