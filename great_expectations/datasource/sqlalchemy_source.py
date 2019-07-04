import time
import logging
from string import Template

from .datasource import Datasource
from great_expectations.dataset.sqlalchemy_dataset import SqlAlchemyDataset
from .generator.query_generator import QueryGenerator

logger = logging.getLogger(__name__)

try:
    import sqlalchemy
    from sqlalchemy import create_engine, MetaData
except ImportError:
    sqlalchemy = None
    create_engine = None
    MetaData = None
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

    def __init__(self, name="default", data_context=None, profile=None, generators=None, **kwargs):
        if generators is None:
            generators = {
                "default": {"type": "queries"}
            }
        super(SqlAlchemyDatasource, self).__init__(name,
                                                   type_="sqlalchemy",
                                                   data_context=data_context,
                                                   generators=generators)
        if profile is not None:
            self._datasource_config.update({
                "profile": profile
            })
        # if an engine was provided, use that
        if "engine" in kwargs:
            self.engine = kwargs.pop("engine")

        # if a connection string or url was provided, use that
        elif "connection_string" in kwargs:
            connection_string = kwargs.pop("connection_string")
            self.engine = create_engine(connection_string, **kwargs)
        elif "url" in kwargs:
            url = kwargs.pop("url")
            self.engine = create_engine(url, **kwargs)

        # Otherwise, connect using remaining kwargs
        else:
            self._connect(self._get_sqlalchemy_connection_options(**kwargs))

        self._build_generators()

    def _get_sqlalchemy_connection_options(self, **kwargs):
        if "profile" in self._datasource_config:
            profile = self._datasource_config["profile"]
            credentials = self.data_context.get_profile_credentials(profile)
        else:
            credentials = {}

        # Update credentials with anything passed during connection time
        credentials.update(dict(**kwargs))
        drivername = credentials.pop("drivername")
        options = sqlalchemy.engine.url.URL(drivername, **credentials)
        return options

    def _connect(self, options):
        self.engine = create_engine(options)
        self.meta = MetaData()

    def _get_generator_class(self, type_):
        if type_ == "queries":
            return QueryGenerator
        else:
            raise ValueError("Unrecognized DataAssetGenerator type %s" % type_)

    def _get_data_asset(self, batch_kwargs, expectation_suite, schema=None, **kwargs):
        if "table" in batch_kwargs:
            return SqlAlchemyDataset(table_name=batch_kwargs["table"], 
                                     engine=self.engine,
                                     schema=schema,
                                     data_context=self._data_context,
                                     expectation_suite=expectation_suite,
                                     batch_kwargs=batch_kwargs)

        elif "query" in batch_kwargs:
            query = Template(batch_kwargs["query"]).safe_substitute(**kwargs)
            return SqlAlchemyDataset(custom_sql=query,
                                     engine=self.engine,
                                     data_context=self._data_context,
                                     expectation_suite=expectation_suite,
                                     batch_kwargs=batch_kwargs)
    
        else:
            raise ValueError("Invalid batch_kwargs: exactly one of 'table' or 'query' must be specified")

    def build_batch_kwargs(self, *args, **kwargs):
        """Magically build batch_kwargs by guessing that the first non-keyword argument is a table name"""
        if len(args) > 0:
            kwargs.update({
                "table": args[0],
                "timestamp": time.time()
            })
        return kwargs
