import time
import logging
from string import Template

from great_expectations.datasource import Datasource
from great_expectations.datasource.types import BatchId
from great_expectations.dataset.sqlalchemy_dataset import SqlAlchemyDataset
from .generator.query_generator import QueryGenerator
from great_expectations.exceptions import DatasourceInitializationError
from great_expectations.types import ClassConfig

logger = logging.getLogger(__name__)

try:
    import sqlalchemy
    from sqlalchemy import create_engine
except ImportError:
    sqlalchemy = None
    create_engine = None
    logger.debug("Unable to import sqlalchemy.")


class SqlAlchemyDatasource(Datasource):
    """
    A SqlAlchemyDatasource will provide data_assets converting batch_kwargs using the following rules:
      - if the batch_kwargs include a table key, the datasource will provide a dataset object connected
        to that table
      - if the batch_kwargs include a query key, the datasource will create a temporary table using that
        that query. The query can be parameterized according to the standard python Template engine, which
        uses $parameter, with additional kwargs passed to the get_batch method.
    """

    @classmethod
    def build_configuration(cls, data_asset_type=None, generators=None, **kwargs):
        """
        Build a full configuration object for a datasource, potentially including generators with defaults.

        Args:
            data_asset_type: A ClassConfig dictionary
            generators: Generator configuration dictionary
            **kwargs: Additional kwargs to be part of the datasource constructor's initialization

        Returns:
            A complete datasource configuration.

        """
        if generators is None:
            generators = {
                "default": {
                    "class_name": "TableGenerator"
                }
            }

        if data_asset_type is None:
            data_asset_type = ClassConfig(
                class_name="SqlAlchemyDataset")
        else:
            try:
                data_asset_type = ClassConfig(**data_asset_type)
            except TypeError:
                # In this case, we allow the passed config, for now, in case they're using a legacy string-only config
                pass

        configuration = kwargs
        configuration.update({
            "data_asset_type": data_asset_type,
            "generators": generators,
        })
        return configuration

    def __init__(self, name="default", data_context=None, data_asset_type=None, credentials=None, generators=None, **kwargs):
        if not sqlalchemy:
            raise DatasourceInitializationError(name, "ModuleNotFoundError: No module named 'sqlalchemy'")

        configuration_with_defaults = SqlAlchemyDatasource.build_configuration(data_asset_type, generators, **kwargs)
        data_asset_type = configuration_with_defaults.pop("data_asset_type")
        generators = configuration_with_defaults.pop("generators")
        super(SqlAlchemyDatasource, self).__init__(
            name,
            data_context=data_context,
            data_asset_type=data_asset_type,
            generators=generators,
            **configuration_with_defaults)

        if credentials is not None:
            self._datasource_config.update({
                "credentials": credentials
            })
        else:
            credentials = {}

        try:
            # if an engine was provided, use that
            if "engine" in kwargs:
                self.engine = kwargs.pop("engine")

            # if a connection string or url was provided, use that
            elif "connection_string" in kwargs:
                connection_string = kwargs.pop("connection_string")
                self.engine = create_engine(connection_string, **kwargs)
                self.engine.connect()
            elif "url" in credentials:
                url = credentials.pop("url")
                # TODO perhaps we could carefully regex out the driver from the
                #  url. It would need to be cautious to avoid leaking secrets.
                self.drivername = "other"
                self.engine = create_engine(url, **kwargs)
                self.engine.connect()

            # Otherwise, connect using remaining kwargs
            else:
                options, drivername = self._get_sqlalchemy_connection_options(**kwargs)
                self.drivername = drivername
                self.engine = create_engine(options)
                self.engine.connect()

        except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.DatabaseError) as sqlalchemy_error:
            raise DatasourceInitializationError(self._name, str(sqlalchemy_error))

        self._build_generators()

    def _get_sqlalchemy_connection_options(self, **kwargs):
        drivername = None
        if "credentials" in self._datasource_config:
            credentials = self._datasource_config["credentials"]
        else:
            credentials = {}

        # if a connection string or url was provided in the profile, use that
        if "connection_string" in credentials:
            options = credentials["connection_string"]
        elif "url" in credentials:
            options = credentials["url"]
        else:
            # Update credentials with anything passed during connection time
            credentials.update(dict(**kwargs))
            drivername = credentials.pop("drivername")
            options = sqlalchemy.engine.url.URL(drivername, **credentials)

        return options, drivername

    def _get_generator_class_from_type(self, type_):
        if type_ == "queries":
            return QueryGenerator
        else:
            raise ValueError("Unrecognized DataAssetGenerator type %s" % type_)

    def _get_data_asset(self, batch_kwargs, expectation_suite, **kwargs):
        batch_kwargs.update(kwargs)

        if "data_asset_type" in batch_kwargs:
            # Sqlalchemy does not use reader_options or need to remove batch_kwargs since it does not pass
            # options through to a later reader
            data_asset_type_config = batch_kwargs["data_asset_type"]
            try:
                data_asset_type_config = ClassConfig(**data_asset_type_config)
            except TypeError:
                # We tried; we'll pass the config downstream, probably as a string, and handle an error later
                pass
        else:
            data_asset_type_config = self._data_asset_type

        data_asset_type = self._get_data_asset_class(data_asset_type_config)

        if not issubclass(data_asset_type, SqlAlchemyDataset):
            raise ValueError("SqlAlchemyDatasource cannot instantiate batch with data_asset_type: '%s'. It "
                             "must be a subclass of SqlAlchemyDataset." % data_asset_type.__name__)

        # We need to build a batch_id to be used in the dataframe
        batch_id = BatchId({
            "timestamp": time.time()
        })

        if "schema" in batch_kwargs:
            schema = batch_kwargs["schema"]
        else:
            schema = None

        if "table" in batch_kwargs:
            return data_asset_type(
                table_name=batch_kwargs["table"],
                engine=self.engine,
                schema=schema,
                data_context=self._data_context,
                expectation_suite=expectation_suite,
                batch_kwargs=batch_kwargs,
                batch_id=batch_id
            )

        elif "query" in batch_kwargs:
            query = Template(batch_kwargs["query"]).safe_substitute(**kwargs)
            return data_asset_type(
                custom_sql=query,
                engine=self.engine,
                data_context=self._data_context,
                expectation_suite=expectation_suite,
                batch_kwargs=batch_kwargs,
                batch_id=batch_id
            )
    
        else:
            raise ValueError("Invalid batch_kwargs: exactly one of 'table' or 'query' must be specified")
