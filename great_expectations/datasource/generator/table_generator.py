import os
import logging
from string import Template

from marshmallow import Schema, fields, post_load, ValidationError

from .batch_generator import BatchGenerator
from great_expectations.exceptions import BatchKwargsError, GreatExpectationsError
from great_expectations.datasource.types import SqlAlchemyDatasourceTableBatchKwargs


logger = logging.getLogger(__name__)

try:
    import sqlalchemy
    from sqlalchemy import create_engine
    from sqlalchemy.engine import reflection
except ImportError:
    sqlalchemy = None
    create_engine = None
    reflection = None
    logger.debug("Unable to import sqlalchemy.")


class AssetConfigurationSchema(Schema):
    table = fields.Str()
    schema = fields.Str()

    @post_load(pass_many=False)
    def make_asset_configuration(self, data):
        return AssetConfiguration(**data)


class AssetConfiguration(object):
    def __init__(self, table, schema=None):
        self.__table = table
        self.__schema = schema

    @property
    def table(self):
        return self.__table

    @property
    def schema(self):
        return self.__schema


assetConfigurationSchema = AssetConfigurationSchema()


class TableGenerator(BatchGenerator):
    """Provide access to already materialized tables or views in a database.

    TableGenerator can be used to define specific data asset names that take and substitute parameters,
    for example to support referring to the same data asset but with different schemas depending on provided
    batch_kwargs.

    The python template language is used to substitute table name portions. For example, consider the
    following configurations::

        my_generator:
          class_name: TableGenerator
          assets:
            my_table:
              schema: $schema
              table: my_table


    In that case, the asset my_datasource/my_generator/my_asset will refer to a table called my_table in a schema
    defined in batch_kwargs.

    """

    def __init__(self, name="default", datasource=None, assets=None):
        super(TableGenerator, self).__init__(name=name, datasource=datasource)
        if not assets:
            assets = {}
        try:
            self._assets = {
                asset_name: assetConfigurationSchema.load(asset_config).data for
                (asset_name, asset_config) in assets.items()
            }
        except ValidationError as err:
            raise GreatExpectationsError("Unable to load asset configuration in TableGenerator '%s': "
                                         "validation error: %s." % (name, str(err)))

        if datasource is not None:
            self.engine = datasource.engine
            try:
                self.inspector = sqlalchemy.inspect(self.engine)

            except sqlalchemy.exc.OperationalError:
                logger.warning("Unable to create inspector from engine in generator '%s'" % name)
                self.inspector = None

    def _get_iterator(self, generator_asset, **kwargs):
        # First, we check if we have a configured asset
        if generator_asset in self._assets:
            asset_config = self._assets[generator_asset]
            try:
                table_name = Template(asset_config.table).substitute(kwargs)
                schema_name = None
                if asset_config.schema is not None:
                    schema_name = Template(asset_config.schema).substitute(kwargs)
            except KeyError:
                raise BatchKwargsError("Unable to generate batch kwargs for asset '" + generator_asset + "': "
                                       "missing template key",
                                       {"generator_asset": generator_asset,
                                        "table_template": asset_config.table,
                                        "schema_template": asset_config.schema}
                                       )
            return iter([
                SqlAlchemyDatasourceTableBatchKwargs(
                    table=table_name,
                    schema=schema_name
                )
            ])

        # If this is not a manually configured asset, we fall back to inspection of the database
        elif self.engine is not None and self.inspector is not None:
            split_generator_asset = generator_asset.split(".")
            if len(split_generator_asset) == 2:
                schema_name = split_generator_asset[0]
                table_name = split_generator_asset[1]
            elif len(split_generator_asset) == 1:
                schema_name = self.inspector.default_schema_name
                table_name = split_generator_asset[0]
            else:
                raise ValueError("Table name must be of shape '[SCHEMA.]TABLE'. Passed: " + split_generator_asset)
            tables = self.inspector.get_table_names(schema=schema_name)
            tables.extend(self.inspector.get_view_names(schema=schema_name))
            if table_name in tables:
                return iter([
                    SqlAlchemyDatasourceTableBatchKwargs(
                        table=table_name,
                        schema=schema_name,
                    )
                ])

    def get_available_data_asset_names(self):
        defined_assets = list(self._assets.keys())
        tables = []
        if self.engine is not None and self.inspector is not None:
            for schema_name in self.inspector.get_schema_names():
                known_information_schemas = [
                    "INFORMATION_SCHEMA",  # snowflake, mssql, mysql, oracle
                    "information_schema",  # postgres, redshift, mysql
                    "performance_schema",  # mysql
                    "sys",                 # mysql
                    "mysql",               # mysql
                ]
                known_system_tables = [
                    "sqlite_master"  # sqlite
                ]
                if schema_name in known_information_schemas:
                    continue

                tables.extend(
                    [table_name if self.inspector.default_schema_name == schema_name else
                     schema_name + "." + table_name
                     for table_name in self.inspector.get_table_names(schema=schema_name)
                     if table_name not in known_system_tables
                     ]
                )
                tables.extend(
                    [table_name if self.inspector.default_schema_name == schema_name else
                     schema_name + "." + table_name
                     for table_name in self.inspector.get_view_names(schema=schema_name)
                     if table_name not in known_system_tables
                     ]
                )

        return defined_assets + tables

    def build_batch_kwargs_from_partition_id(self, generator_asset, partition_id=None, batch_kwargs=None, **kwargs):
        all_the_kwargs = batch_kwargs.copy()
        all_the_kwargs.update(kwargs)
        return next(self._get_iterator(generator_asset, partition_id=partition_id, **all_the_kwargs))

    def get_available_partition_ids(self, generator_asset):
        raise BatchKwargsError("TableGenerator cannot identify partitions, however any existing table may"
                               "already be referenced by accessing a generator_asset with the name of the "
                               "table or of the form SCHEMA.TABLE", {})
