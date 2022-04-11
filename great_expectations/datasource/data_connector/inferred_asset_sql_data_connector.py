from copy import deepcopy
from typing import Dict, List, Optional, Tuple, cast

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import (
    BatchDefinition,
    BatchRequest,
    BatchSpec,
    IDDict,
)
from great_expectations.core.batch_spec import SqlAlchemyDatasourceBatchSpec
from great_expectations.datasource.data_connector.asset import Asset
from great_expectations.datasource.data_connector.configured_asset_sql_data_connector import (
    DataConnector,
)
from great_expectations.datasource.data_connector.util import (
    batch_definition_matches_batch_request,
)
from great_expectations.execution_engine import (
    ExecutionEngine,
    SqlAlchemyExecutionEngine,
)

try:
    import sqlalchemy as sa
    from sqlalchemy.exc import OperationalError
except ImportError:
    sa = None


class InferredAssetSqlDataConnector(DataConnector):
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
    ):
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

        self._introspection_directives = introspection_directives or {}

        super().__init__(
            name=name,
            datasource_name=datasource_name,
            execution_engine=execution_engine,
            assets=None,
            batch_spec_passthrough=batch_spec_passthrough,
        )

        # This cache will contain a "config" for each data_asset discovered via introspection.
        # This approach ensures that ConfiguredAssetSqlDataConnector._assets and _introspected_assets_cache store objects of the same "type"
        # Note: We should probably turn them into AssetConfig objects
        self._introspected_assets_cache = {}
        self._refresh_introspected_assets_cache(
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
    def assets(self) -> Dict[str, Asset]:
        return self._introspected_assets_cache

    def add_data_asset(
        self,
        name: str,
        config: dict,
    ):
        """
        Add data_asset to DataConnector using data_asset name as key, and data_asset config as value.
        """
        name = self._update_data_asset_name_from_config(name, config)
        self._assets[name] = config

    def _update_data_asset_name_from_config(
        self, data_asset_name: str, data_asset_config: dict
    ) -> str:

        data_asset_name_prefix: str = data_asset_config.get(
            "data_asset_name_prefix", ""
        )
        data_asset_name_suffix: str = data_asset_config.get(
            "data_asset_name_suffix", ""
        )
        schema_name: str = data_asset_config.get("schema_name", "")
        include_schema_name: bool = data_asset_config.get("include_schema_name", True)
        if schema_name and include_schema_name is False:
            raise ge_exceptions.DataConnectorError(
                message=f"{self.__class__.__name__} ran into an error while initializing Asset names. Schema {schema_name} was specified, but 'include_schema_name' flag was set to False."
            )

        if schema_name:
            data_asset_name: str = f"{schema_name}.{data_asset_name}"

        data_asset_name: str = (
            f"{data_asset_name_prefix}{data_asset_name}{data_asset_name_suffix}"
        )

        return data_asset_name

    def _get_batch_identifiers_list_from_data_asset_config(
        self,
        data_asset_name,
        data_asset_config,
    ):
        if "table_name" in data_asset_config:
            table_name = data_asset_config["table_name"]
        else:
            table_name = data_asset_name

        if "splitter_method" in data_asset_config:
            splitter_fn = getattr(self, data_asset_config["splitter_method"])
            split_query = splitter_fn(
                table_name=table_name, **data_asset_config["splitter_kwargs"]
            )

            sqlalchemy_execution_engine: SqlAlchemyExecutionEngine = cast(
                SqlAlchemyExecutionEngine, self._execution_engine
            )
            rows = sqlalchemy_execution_engine.engine.execute(split_query).fetchall()

            # Zip up split parameters with column names
            column_names = self._get_column_names_from_splitter_kwargs(
                data_asset_config["splitter_kwargs"]
            )
            batch_identifiers_list = [dict(zip(column_names, row)) for row in rows]

        else:
            batch_identifiers_list = [{}]

        return batch_identifiers_list

    def _get_column_names_from_splitter_kwargs(self, splitter_kwargs) -> List[str]:
        column_names: List[str] = []

        if "column_names" in splitter_kwargs:
            column_names = splitter_kwargs["column_names"]
        elif "column_name" in splitter_kwargs:
            column_names = [splitter_kwargs["column_name"]]

        return column_names

    def get_available_data_asset_names(self) -> List[str]:
        """
        Return the list of asset names known by this DataConnector.

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

    def _get_data_reference_list_from_cache_by_data_asset_name(
        self, data_asset_name: str
    ) -> List[str]:
        return self._data_references_cache[data_asset_name]

    def _map_data_reference_to_batch_definition_list(
        self, data_reference, data_asset_name: Optional[str] = None  #: Any,
    ) -> Optional[List[BatchDefinition]]:
        # Note: This is a bit hacky, but it works. In sql_data_connectors, data references *are* dictionaries,
        # allowing us to invoke `IDDict(data_reference)`
        return [
            BatchDefinition(
                datasource_name=self.datasource_name,
                data_connector_name=self.name,
                data_asset_name=data_asset_name,
                batch_identifiers=IDDict(data_reference),
            )
        ]

    def build_batch_spec(
        self, batch_definition: BatchDefinition
    ) -> SqlAlchemyDatasourceBatchSpec:
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

        return SqlAlchemyDatasourceBatchSpec(batch_spec)

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
        table_name: str = self._get_table_name_from_batch_definition(batch_definition)
        return {
            "data_asset_name": data_asset_name,
            "table_name": table_name,
            "batch_identifiers": batch_definition.batch_identifiers,
            **self.assets[data_asset_name],
        }

    def _get_table_name_from_batch_definition(
        self, batch_definition: BatchDefinition
    ) -> str:
        """
            Helper method called by _get_batch_identifiers_list_from_data_asset_config() to parse table_name from data_asset_name in cases
            where schema is included.

            data_asset_name in those cases are [schema].[table_name].

        function will split data_asset_name on [schema]. and return the resulting table_name.
        """
        table_name: str = batch_definition.data_asset_name
        data_asset_dict: dict = self.assets[batch_definition.data_asset_name]
        if "schema_name" in data_asset_dict:
            schema_name_str: str = data_asset_dict["schema_name"]
            if schema_name_str in table_name:
                table_name = table_name.split(f"{schema_name_str}.")[1]

        return table_name

    # Splitter methods for listing partitions

    def _split_on_whole_table(
        self,
        table_name: str,
    ):
        """
        'Split' by returning the whole table

        Note: the table_name parameter is a required to keep the signature of this method consistent with other methods.
        """
        return sa.select([sa.true()])

    def _split_on_column_value(
        self,
        table_name: str,
        column_name: str,
    ):
        """Split using the values in the named column"""
        # query = f"SELECT DISTINCT(\"{self.column_name}\") FROM {self.table_name}"

        return (
            sa.select([sa.func.distinct(sa.column(column_name))])
            .select_from(sa.text(table_name))
            .order_by(sa.column(column_name).asc())
        )

    def _split_on_converted_datetime(
        self,
        table_name: str,
        column_name: str,
        date_format_string: str = "%Y-%m-%d",
    ):
        """Convert the values in the named column to the given date_format, and split on that"""
        # query = f"SELECT DISTINCT( strftime(\"{date_format_string}\", \"{self.column_name}\")) as my_var FROM {self.table_name}"

        return sa.select(
            [
                sa.func.distinct(
                    sa.func.strftime(
                        date_format_string,
                        sa.column(column_name),
                    )
                )
            ]
        ).select_from(sa.text(table_name))

    def _split_on_divided_integer(
        self, table_name: str, column_name: str, divisor: int
    ):
        """Divide the values in the named column by `divisor`, and split on that"""
        # query = f"SELECT DISTINCT(\"{self.column_name}\" / {divisor}) AS my_var FROM {self.table_name}"

        return sa.select(
            [sa.func.distinct(sa.cast(sa.column(column_name) / divisor, sa.Integer))]
        ).select_from(sa.text(table_name))

    def _split_on_mod_integer(self, table_name: str, column_name: str, mod: int):
        """Divide the values in the named column by `divisor`, and split on that"""
        # query = f"SELECT DISTINCT(\"{self.column_name}\" / {divisor}) AS my_var FROM {self.table_name}"

        return sa.select(
            [sa.func.distinct(sa.cast(sa.column(column_name) % mod, sa.Integer))]
        ).select_from(sa.text(table_name))

    def _split_on_multi_column_values(
        self,
        table_name: str,
        column_names: List[str],
    ):
        """Split on the joint values in the named columns"""
        # query = f"SELECT DISTINCT(\"{self.column_name}\") FROM {self.table_name}"

        return (
            sa.select([sa.column(column_name) for column_name in column_names])
            .distinct()
            .select_from(sa.text(table_name))
        )

    def _split_on_hashed_column(
        self,
        table_name: str,
        column_name: str,
        hash_digits: int,
    ):
        """Note: this method is experimental. It does not work with all SQL dialects."""
        # query = f"SELECT MD5(\"{self.column_name}\") = {matching_hash}) AS hashed_var FROM {self.table_name}"

        return sa.select([sa.func.md5(sa.column(column_name))]).select_from(
            sa.text(table_name)
        )

    def _refresh_data_references_cache(self):
        self._data_references_cache = {}

        for data_asset_name in self.assets:
            data_asset = self.assets[data_asset_name]
            batch_identifiers_list = (
                self._get_batch_identifiers_list_from_data_asset_config(
                    data_asset_name,
                    data_asset,
                )
            )

            # TODO Abe 20201029 : Apply sorters to batch_identifiers_list here
            # TODO Will 20201102 : add sorting code here
            self._data_references_cache[data_asset_name] = batch_identifiers_list

    def _refresh_introspected_assets_cache(
        self,
        data_asset_name_prefix: str = None,
        data_asset_name_suffix: str = None,
        include_schema_name: bool = False,
        splitter_method: str = None,
        splitter_kwargs: dict = None,
        sampling_method: str = None,
        sampling_kwargs: dict = None,
        excluded_tables: List = None,
        included_tables: List = None,
        skip_inapplicable_tables: bool = True,
    ):
        introspected_table_metadata = self._introspect_db(
            **self._introspection_directives
        )
        for metadata in introspected_table_metadata:
            if (excluded_tables is not None) and (
                f"{metadata['schema_name']}.{metadata['table_name']}" in excluded_tables
            ):
                continue

            if (included_tables is not None) and (
                f"{metadata['schema_name']}.{metadata['table_name']}"
                not in included_tables
            ):
                continue

            if include_schema_name:
                data_asset_name = (
                    data_asset_name_prefix
                    + metadata["schema_name"]
                    + "."
                    + metadata["table_name"]
                    + data_asset_name_suffix
                )
            else:
                data_asset_name = (
                    data_asset_name_prefix
                    + metadata["table_name"]
                    + data_asset_name_suffix
                )

            data_asset_config = {
                "schema_name": metadata["schema_name"],
                "table_name": metadata["table_name"],
                "type": metadata["type"],
            }
            if not splitter_method is None:
                data_asset_config["splitter_method"] = splitter_method
            if not splitter_kwargs is None:
                data_asset_config["splitter_kwargs"] = splitter_kwargs
            if not sampling_method is None:
                data_asset_config["sampling_method"] = sampling_method
            if not sampling_kwargs is None:
                data_asset_config["sampling_kwargs"] = sampling_kwargs

            # Attempt to fetch a list of batch_identifiers from the table
            try:
                self._get_batch_identifiers_list_from_data_asset_config(
                    data_asset_name,
                    data_asset_config,
                )
            except OperationalError as e:
                # If it doesn't work, then...
                if skip_inapplicable_tables:
                    # No harm done. Just don't include this table in the list of assets.
                    continue

                else:
                    # We're being strict. Crash now.
                    raise ValueError(
                        f"Couldn't execute a query against table {metadata['table_name']} in schema {metadata['schema_name']}"
                    ) from e

            # Store an asset config for each introspected data asset.
            self._introspected_assets_cache[data_asset_name] = data_asset_config

    def _introspect_db(
        self,
        schema_name: str = None,
        ignore_information_schemas_and_system_tables: bool = True,
        information_schemas: List[str] = [
            "INFORMATION_SCHEMA",  # snowflake, mssql, mysql, oracle
            "information_schema",  # postgres, redshift, mysql
            "performance_schema",  # mysql
            "sys",  # mysql
            "mysql",  # mysql
        ],
        system_tables: List[str] = ["sqlite_master"],  # sqlite
        include_views=True,
    ):
        engine = self._execution_engine.engine
        inspector = sa.inspect(engine)

        selected_schema_name = schema_name

        tables = []
        for schema_name in inspector.get_schema_names():
            if (
                ignore_information_schemas_and_system_tables
                and schema_name in information_schemas
            ):
                continue

            if selected_schema_name is not None and schema_name != selected_schema_name:
                continue

            for table_name in inspector.get_table_names(schema=schema_name):

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
            if "redshift" == engine.dialect.name.lower():
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
            if not "UndefinedTable" in str(e):
                raise e

        return tables

    def get_available_data_asset_names_and_types(self) -> List[Tuple[str, str]]:
        """
        Return the list of asset names and types known by this DataConnector.

        Returns:
            A list of tuples consisting of available names and types
        """
        return [(asset["table_name"], asset["type"]) for asset in self.assets.values()]

    def get_batch_definition_list_from_batch_request(self, batch_request: BatchRequest):
        sub_cache: dict

        self._validate_batch_request(batch_request=batch_request)

        if len(self._data_references_cache) == 0:
            self._refresh_data_references_cache()

        batch_definition_list: List[BatchDefinition] = []
        try:
            sub_cache = self._data_references_cache[batch_request.data_asset_name]
        except KeyError:
            raise KeyError(
                f"data_asset_name {batch_request.data_asset_name} is not recognized."
            )

        for batch_identifiers in sub_cache:
            batch_definition: BatchDefinition = BatchDefinition(
                datasource_name=self.datasource_name,
                data_connector_name=self.name,
                data_asset_name=batch_request.data_asset_name,
                batch_identifiers=IDDict(batch_identifiers),
                batch_spec_passthrough=batch_request.batch_spec_passthrough,
            )
            if batch_definition_matches_batch_request(batch_definition, batch_request):
                batch_definition_list.append(batch_definition)

        return batch_definition_list
