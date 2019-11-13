import logging
from string import Template

from marshmallow import post_load, fields, Schema, ValidationError

from .batch_generator import BatchGenerator
from great_expectations.datasource.types import SqlAlchemyDatasourceQueryBatchKwargs
from great_expectations.exceptions import BatchKwargsError, GreatExpectationsError

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
    schema = fields.Str(allow_none=True)
    count = fields.Integer(allow_none=True)
    limit = fields.Integer(allow_none=True, validate=lambda x: x > 0)

    @post_load(pass_many=False)
    def make_asset_configuration(self, data):
        return AssetConfiguration(**data)


class AssetConfiguration(object):
    def __init__(self, table, schema=None, count=None, limit=None):
        self.__table = table
        self.__schema = schema
        self.count = count  # count is intended to be mutable
        self.limit = limit  # limit is intended to be mutable

    @property
    def table(self):
        return self.__table

    @property
    def schema(self):
        return self.__schema


assetConfigurationSchema = AssetConfigurationSchema(strict=True, exclude=['count'])


class SamplingTableGenerator(BatchGenerator):
    """Produce query-based batch_kwargs based from tables, with sampling parameters applied
    """

    def __init__(self, name="default", datasource=None, limit=None, assets=None):
        super(SamplingTableGenerator, self).__init__(name=name, datasource=datasource)
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
        self._limit = limit

    def _get_asset_config(self, generator_asset, limit=None):

        asset_config = self._assets.get(generator_asset)
        if asset_config is None and (self.engine and self.inspector):
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
            try:
                tables.extend(self.inspector.get_view_names(schema=schema_name))
            except NotImplementedError:
                pass
            if table_name in tables:
                asset_config = AssetConfiguration(
                    table=table_name,
                    schema=schema_name,
                    limit=self._limit,
                )
        if asset_config is None:
            raise BatchKwargsError("Unable to generate an asset config for %s" % generator_asset)

        return asset_config

    def _get_iterator(self, generator_asset, partition_id=0, limit=None, **kwargs):
        asset_config = self._get_asset_config(generator_asset)
        if limit:
            asset_config.limit = limit
        raw_query = sqlalchemy.select([sqlalchemy.text("*")])\
            .select_from(sqlalchemy.schema.Table(asset_config.table, sqlalchemy.MetaData(), schema=asset_config.schema))\
            .offset(partition_id)\
            .limit(asset_config.limit)

        if self.engine:
            return iter([
                SqlAlchemyDatasourceQueryBatchKwargs(
                    query=str(raw_query.compile(self.engine, compile_kwargs={"literal_binds": True})),
                )
                ])
        else:
            return iter([
                SqlAlchemyDatasourceQueryBatchKwargs(
                    query=str(raw_query)
                )])

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
                try:
                    tables.extend(
                        [table_name if self.inspector.default_schema_name == schema_name else
                         schema_name + "." + table_name
                         for table_name in self.inspector.get_view_names(schema=schema_name)
                         if table_name not in known_system_tables
                         ]
                    )
                except NotImplementedError:
                    pass

        return defined_assets + tables

    def build_batch_kwargs_from_partition_id(self, generator_asset, partition_id=None, batch_kwargs=None, **kwargs):
        """Build batch kwargs from a partition id."""
        raw_query = self._get_raw_query(generator_asset)
        if "$partition_id" not in raw_query and "${partition_id}" not in raw_query:
            raise BatchKwargsError("No partition_id parameter found in the requested query.", {})
        try:
            substituted_query = Template(raw_query).substitute({"partition_id": partition_id})
        except KeyError:
            raise BatchKwargsError(
                "Unable to generate batch kwargs for asset '" + generator_asset + "': "
                                                                                  "missing template key",
                {
                    "generator_asset": generator_asset,
                    "query_template": raw_query
                }
            )
        return SqlAlchemyDatasourceQueryBatchKwargs(
            query=substituted_query
        )

    def get_available_partition_ids(self, generator_asset):
        if not self.engine:
            raise BatchKwargsError("SamplingTableGenerator cannot identify partition ids without an engine.")
        asset_config = self._get_asset_config(generator_asset)
        if not asset_config.count:
            count_query = sqlalchemy.select([sqlalchemy.func.count()]).select_from(
                asset_config.table, schema=asset_config.schema)
            asset_config.count = self.engine.execute(count_query).scalar()
        return range(0, asset_config.count, asset_config.count // 10)