import logging
import warnings
from string import Template

from great_expectations.datasource.batch_kwargs_generator.batch_kwargs_generator import (
    BatchKwargsGenerator,
)
from great_expectations.datasource.types import SqlAlchemyDatasourceTableBatchKwargs
from great_expectations.exceptions import BatchKwargsError, GreatExpectationsError
from great_expectations.marshmallow__shade import (
    Schema,
    ValidationError,
    fields,
    post_load,
)

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

    @post_load
    def make_asset_configuration(self, data, **kwargs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return AssetConfiguration(**data)


class AssetConfiguration:
    def __init__(self, table, schema=None) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        self.__table = table
        self.__schema = schema

    @property
    def table(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.__table

    @property
    def schema(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self.__schema


assetConfigurationSchema = AssetConfigurationSchema()


class TableBatchKwargsGenerator(BatchKwargsGenerator):
    "Provide access to already materialized tables or views in a database.\n\n    TableBatchKwargsGenerator can be used to define specific data asset names that take and substitute parameters,\n    for example to support referring to the same data asset but with different schemas depending on provided\n    batch_kwargs.\n\n    The python template language is used to substitute table name portions. For example, consider the\n    following configurations::\n\n        my_generator:\n          class_name: TableBatchKwargsGenerator\n          assets:\n            my_table:\n              schema: $schema\n              table: my_table\n\n\n    In that case, the asset my_datasource/my_generator/my_asset will refer to a table called my_table in a schema\n    defined in batch_kwargs.\n\n"
    recognized_batch_parameters = {
        "data_asset_name",
        "limit",
        "offset",
        "query_parameters",
    }

    def __init__(self, name="default", datasource=None, assets=None) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
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
                "Unable to load asset configuration in TableBatchKwargsGenerator '%s': validation error: %s."
                % (name, str(err))
            )
        if datasource is not None:
            self.engine = datasource.engine
            try:
                self.inspector = sqlalchemy.inspect(self.engine)
            except sqlalchemy.exc.OperationalError:
                logger.warning(
                    "Unable to create inspector from engine in batch kwargs generator '%s'"
                    % name
                )
                self.inspector = None

    def _get_iterator(
        self,
        data_asset_name,
        query_parameters=None,
        limit=None,
        offset=None,
        partition_id=None,
    ):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        batch_kwargs = None
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
                    (
                        (
                            "Unable to generate batch kwargs for asset '"
                            + data_asset_name
                        )
                        + "': missing template key"
                    ),
                    {
                        "data_asset_name": data_asset_name,
                        "table_template": asset_config.table,
                        "schema_template": asset_config.schema,
                    },
                )
            batch_kwargs = SqlAlchemyDatasourceTableBatchKwargs(
                table=table_name, schema=schema_name
            )
        elif (self.engine is not None) and (self.inspector is not None):
            project_id = None
            schema_name = None
            split_data_asset_name = data_asset_name.split(".")
            if len(split_data_asset_name) == 2:
                schema_name = split_data_asset_name[0]
                if self.engine.dialect.name.lower() == "bigquery":
                    table_name = data_asset_name
                else:
                    table_name = split_data_asset_name[1]
            elif len(split_data_asset_name) == 1:
                schema_name = self.inspector.default_schema_name
                table_name = split_data_asset_name[0]
            elif (len(split_data_asset_name) == 3) and (
                self.engine.dialect.name.lower() == "bigquery"
            ):
                project_id = split_data_asset_name[0]
                schema_name = split_data_asset_name[1]
                table_name = data_asset_name
            else:
                shape = "[SCHEMA.]TABLE"
                if self.engine.dialect.name.lower() == "bigquery":
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
                    f"TableBatchKwargsGenerator cannot access the following data:SCHEMA : {schema_name}TABLE : {table_name}",
                    {},
                )
        if batch_kwargs is not None:
            if partition_id is not None:
                logger.warning(
                    "table_generator cannot identify partitions; provided partition id will be recorded only"
                )
                batch_kwargs["partition_id"] = partition_id
            if limit is not None:
                batch_kwargs["limit"] = limit
            if offset is not None:
                batch_kwargs["offset"] = offset
            return iter([batch_kwargs])
        return

    def get_available_data_asset_names(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        is_complete_list = True
        defined_assets = list(self._assets.keys())
        tables = []
        if (self.engine is not None) and (self.inspector is not None):
            for schema_name in self.inspector.get_schema_names():
                known_information_schemas = [
                    "INFORMATION_SCHEMA",
                    "information_schema",
                    "performance_schema",
                    "sys",
                    "mysql",
                ]
                known_system_tables = ["sqlite_master"]
                if schema_name in known_information_schemas:
                    continue
                if self.engine.dialect.name.lower() == "bigquery":
                    tables.extend(
                        [
                            (table_name, "table")
                            for table_name in self.inspector.get_table_names(
                                schema=schema_name
                            )
                            if (table_name not in known_system_tables)
                        ]
                    )
                else:
                    if self.engine.dialect.name.lower() == "sqlite":
                        default_schema_name = None
                    else:
                        default_schema_name = self.inspector.default_schema_name
                    tables.extend(
                        [
                            (
                                (table_name, "table")
                                if (default_schema_name == schema_name)
                                else (f"{schema_name}.{table_name}", "table")
                            )
                            for table_name in self.inspector.get_table_names(
                                schema=schema_name
                            )
                            if (table_name not in known_system_tables)
                        ]
                    )
                try:
                    tables.extend(
                        [
                            (
                                (table_name, "view")
                                if (default_schema_name == schema_name)
                                else (f"{schema_name}.{table_name}", "view")
                            )
                            for table_name in self.inspector.get_view_names(
                                schema=schema_name
                            )
                            if (table_name not in known_system_tables)
                        ]
                    )
                except NotImplementedError:
                    pass
        return {
            "names": (defined_assets + tables),
            "is_complete_list": is_complete_list,
        }

    def _build_batch_kwargs(self, batch_parameters):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return next(
            self._get_iterator(
                data_asset_name=batch_parameters.get("data_asset_name"),
                query_parameters=batch_parameters.get("query_parameters", {}),
                limit=batch_parameters.get("limit"),
                offset=batch_parameters.get("offset"),
            )
        )

    def get_available_partition_ids(
        self, generator_asset=None, data_asset_name=None
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        assert (generator_asset and (not data_asset_name)) or (
            (not generator_asset) and data_asset_name
        ), "Please provide either generator_asset or data_asset_name."
        if generator_asset:
            warnings.warn(
                "The 'generator_asset' argument is deprecated as of v0.11.0 and will be removed in v0.16. Please use 'data_asset_name' instead.",
                DeprecationWarning,
            )
        raise BatchKwargsError(
            "TableBatchKwargsGenerator cannot identify partitions, however any existing table mayalready be referenced by accessing a data_asset with the name of the table or of the form SCHEMA.TABLE",
            {},
        )
