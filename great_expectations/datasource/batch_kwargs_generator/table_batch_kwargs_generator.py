import logging
from string import Template

from marshmallow import Schema, ValidationError, fields, post_load

from great_expectations.compatibility import sqlalchemy
from great_expectations.datasource.batch_kwargs_generator.batch_kwargs_generator import (
    BatchKwargsGenerator,
)
from great_expectations.datasource.types import SqlAlchemyDatasourceTableBatchKwargs
from great_expectations.exceptions import BatchKwargsError, GreatExpectationsError
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect

logger = logging.getLogger(__name__)


class AssetConfigurationSchema(Schema):
    table = fields.Str()
    schema = fields.Str()

    @post_load
    def make_asset_configuration(self, data, **kwargs):
        return AssetConfiguration(**data)


class AssetConfiguration:
    def __init__(self, table, schema=None) -> None:
        self.__table = table
        self.__schema = schema

    @property
    def table(self):
        return self.__table

    @property
    def schema(self):
        return self.__schema


assetConfigurationSchema = AssetConfigurationSchema()


class TableBatchKwargsGenerator(BatchKwargsGenerator):
    """Provide access to already materialized tables or views in a database.

    TableBatchKwargsGenerator can be used to define specific data asset names that take and substitute parameters,
    for example to support referring to the same data asset but with different schemas depending on provided
    batch_kwargs.

    The python template language is used to substitute table name portions. For example, consider the
    following configurations::

        my_generator:
          class_name: TableBatchKwargsGenerator
          assets:
            my_table:
              schema: $schema
              table: my_table


    In that case, the asset my_datasource/my_generator/my_asset will refer to a table called my_table in a schema
    defined in batch_kwargs.

    """

    recognized_batch_parameters = {
        "data_asset_name",
        "limit",
        "offset",
        "query_parameters",
    }

    def __init__(self, name="default", datasource=None, assets=None) -> None:
        super().__init__(name=name, datasource=datasource)
        if not assets:
            assets = {}
        try:
            self._assets = {
                asset_name: assetConfigurationSchema.load(asset_config)
                for (asset_name, asset_config) in assets.items()
            }
        except ValidationError as err:
            raise GreatExpectationsError(
                f"Unable to load asset configuration in TableBatchKwargsGenerator '{name}': "
                f"validation error: {str(err)}."
            )

        if datasource is not None:
            self.engine = datasource.engine
            try:
                self.inspector = sqlalchemy.Inspector.inspect(self.engine)

            except sqlalchemy.OperationalError:
                logger.warning(
                    f"Unable to create inspector from engine in batch kwargs generator '{name}'"
                )
                self.inspector = None

    def _get_iterator(  # noqa: C901, PLR0912, PLR0913, PLR0915
        self,
        data_asset_name,
        query_parameters=None,
        limit=None,
        offset=None,
        partition_id=None,
    ):
        batch_kwargs = None
        # First, we check if we have a configured asset
        if data_asset_name in self._assets:
            asset_config = self._assets[data_asset_name]
            try:
                if query_parameters is None:
                    query_parameters = {}
                table_name = Template(asset_config.table).substitute(query_parameters)
                schema_name = None
                if asset_config.schema is not None:
                    schema_name = Template(asset_config.schema).substitute(
                        query_parameters
                    )
            except KeyError:
                raise BatchKwargsError(
                    "Unable to generate batch kwargs for asset '"
                    + data_asset_name
                    + "': "
                    "missing template key",
                    {
                        "data_asset_name": data_asset_name,
                        "table_template": asset_config.table,
                        "schema_template": asset_config.schema,
                    },
                )
            batch_kwargs = SqlAlchemyDatasourceTableBatchKwargs(
                table=table_name, schema=schema_name
            )

        # If this is not a manually configured asset, we fall back to inspection of the database
        elif self.engine is not None and self.inspector is not None:
            project_id = None
            schema_name = None
            split_data_asset_name = data_asset_name.split(".")
            if len(split_data_asset_name) == 2:  # noqa: PLR2004
                schema_name = split_data_asset_name[0]
                if self.engine.dialect.name.lower() == GXSqlDialect.BIGQUERY:
                    table_name = data_asset_name
                else:
                    table_name = split_data_asset_name[1]
            elif len(split_data_asset_name) == 1:
                schema_name = self.inspector.default_schema_name
                table_name = split_data_asset_name[0]

            elif (
                len(split_data_asset_name) == 3  # noqa: PLR2004
                and self.engine.dialect.name.lower() == GXSqlDialect.BIGQUERY
            ):
                project_id = split_data_asset_name[0]  # noqa: F841
                schema_name = split_data_asset_name[1]
                table_name = data_asset_name
            else:
                shape = "[SCHEMA.]TABLE"
                if self.engine.dialect.name.lower() == GXSqlDialect.BIGQUERY:
                    shape = f"[PROJECT_ID.]{shape}"

                raise ValueError(
                    "Table name must be of shape '{}'. Passed: {}".format(
                        shape, split_data_asset_name
                    )
                )

            try:
                has_table = self.inspector.has_table
            except AttributeError:
                has_table = self.engine.has_table

            if has_table(table_name, schema=schema_name):
                batch_kwargs = SqlAlchemyDatasourceTableBatchKwargs(
                    table=table_name, schema=schema_name
                )
            else:
                raise BatchKwargsError(
                    "TableBatchKwargsGenerator cannot access the following data:"
                    f"SCHEMA : {schema_name}"
                    f"TABLE : {table_name}",
                    {},
                )

        if batch_kwargs is not None:
            if partition_id is not None:
                logger.warning(
                    "table_generator cannot identify partitions; provided partition id will be recorded "
                    "only"
                )
                batch_kwargs["partition_id"] = partition_id
            if limit is not None:
                batch_kwargs["limit"] = limit
            if offset is not None:
                batch_kwargs["offset"] = offset
            return iter([batch_kwargs])
        # Otherwise, we return None
        return

    def get_available_data_asset_names(self):
        # TODO: limit and is_complete_list logic
        is_complete_list = True
        defined_assets = list(self._assets.keys())
        tables = []
        if self.engine is not None and self.inspector is not None:
            for schema_name in self.inspector.get_schema_names():
                known_information_schemas = [
                    "INFORMATION_SCHEMA",  # snowflake, mssql, mysql, oracle
                    "information_schema",  # postgres, redshift, mysql
                    "performance_schema",  # mysql
                    "sys",  # mysql
                    "mysql",  # mysql
                ]
                known_system_tables = ["sqlite_master"]  # sqlite
                if schema_name in known_information_schemas:
                    continue

                if self.engine.dialect.name.lower() == GXSqlDialect.BIGQUERY:
                    tables.extend(
                        [
                            (table_name, "table")
                            for table_name in self.inspector.get_table_names(
                                schema=schema_name
                            )
                            if table_name not in known_system_tables
                        ]
                    )
                else:
                    # set default_schema_name
                    if self.engine.dialect.name.lower() == GXSqlDialect.SQLITE:
                        # Workaround for compatibility with sqlalchemy < 1.4.0 and is described in issue #2641
                        default_schema_name = None
                    else:
                        default_schema_name = self.inspector.default_schema_name

                    tables.extend(
                        [
                            (table_name, "table")
                            if default_schema_name == schema_name
                            else (f"{schema_name}.{table_name}", "table")
                            for table_name in self.inspector.get_table_names(
                                schema=schema_name
                            )
                            if table_name not in known_system_tables
                        ]
                    )
                try:
                    tables.extend(
                        [
                            (table_name, "view")
                            if default_schema_name == schema_name
                            else (f"{schema_name}.{table_name}", "view")
                            for table_name in self.inspector.get_view_names(
                                schema=schema_name
                            )
                            if table_name not in known_system_tables
                        ]
                    )
                except NotImplementedError:
                    # Not implemented by bigquery dialect
                    pass

        return {"names": defined_assets + tables, "is_complete_list": is_complete_list}

    def _build_batch_kwargs(self, batch_parameters):
        return next(
            self._get_iterator(
                data_asset_name=batch_parameters.get("data_asset_name"),
                query_parameters=batch_parameters.get("query_parameters", {}),
                limit=batch_parameters.get("limit"),
                offset=batch_parameters.get("offset"),
            )
        )

    def get_available_partition_ids(self, data_asset_name=None) -> None:
        raise BatchKwargsError(
            "TableBatchKwargsGenerator cannot identify partitions, however any existing table may"
            "already be referenced by accessing a data_asset with the name of the "
            "table or of the form SCHEMA.TABLE",
            {},
        )
