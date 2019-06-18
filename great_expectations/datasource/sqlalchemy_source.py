import time
import os
import logging
from string import Template

from .datasource import Datasource
from .batch_generator import BatchGenerator
from great_expectations.dataset.sqlalchemy_dataset import SqlAlchemyDataset

logger = logging.getLogger(__name__)

try:
    import sqlalchemy
    from sqlalchemy import create_engine, MetaData
except ImportError:
    sqlalchemy = None
    create_engine = None
    MetaData = None
    logger.debug("Unable to import sqlalchemy.")


class QueryGenerator(BatchGenerator):
    """Produce query-style batch_kwargs from sql files stored on disk
    """

    def __init__(self, datasource, name="default"):
        # TODO: Add tests for QueryGenerator
        super(QueryGenerator, self).__init__(name=name, type_="queries", datasource=datasource)
        self.meta = MetaData()
        if datasource is not None and datasource.data_context is not None:
            self._queries_path = os.path.join(self._datasource.data_context.root_directory,
                                              "datasources",
                                              self._datasource.name,
                                              "generators",
                                              self._name,
                                              "queries")
        else:
            self._queries_path = None
            self._queries = {}

        if datasource is not None:
            self.engine = datasource.engine

    def _get_iterator(self, data_asset_name, **kwargs):
        if self._queries_path:
            if data_asset_name in [path for path in os.walk(self._queries_path) if str(path).endswith(".sql")]:
                with open(os.path.join(self._queries_path, data_asset_name) + ".sql", "r") as data:
                    return iter([{
                        "query": data.read(),
                        "timestamp": time.time()
                    }])
        else:
            if data_asset_name in self._queries:
                return iter([{
                    "query": self._queries[data_asset_name],
                    "timestamp": time.time()
                }])

        if self.engine is not None:
            self.meta.reflect(bind=self.engine)
            tables = [str(table) for table in self.meta.sorted_tables]
            if data_asset_name in tables:
                return iter([
                    {
                        "table": data_asset_name,
                        "timestamp": time.time()
                    }
                ])

    def add_query(self, data_asset_name, query):
        if self._queries_path:
            with open(os.path.join(self._queries_path, data_asset_name + ".sql"), "w") as queryfile:
                queryfile.write(query)
        else:
            logger.info("Adding query to temporary storage only.")
            self._queries[data_asset_name] = query

    def get_available_data_asset_names(self):
        if self._queries_path:
            defined_queries = [path for path in os.walk(self._queries_path) if str(path).endswith(".sql")]
        else:
            defined_queries = list(self._queries.keys())
        if self.engine is not None:
            self.meta.reflect(bind=self.engine)
            tables = [str(table) for table in self.meta.sorted_tables]
        else:
            tables = []

        return set(defined_queries + tables)


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

    def _get_data_asset(self, data_asset_name, batch_kwargs, expectation_suite, schema=None, **kwargs):
        if "table" in batch_kwargs:
            return SqlAlchemyDataset(table_name=batch_kwargs["table"], 
                                     engine=self.engine,
                                     schema=schema,
                                     data_context=self._data_context,
                                     data_asset_name=data_asset_name,
                                     expectation_suite=expectation_suite,
                                     batch_kwargs=batch_kwargs)

        elif "query" in batch_kwargs:
            query = Template(batch_kwargs["query"]).safe_substitute(**kwargs)
            return SqlAlchemyDataset(table_name=data_asset_name, 
                                     engine=self.engine,
                                     data_context=self._data_context,
                                     data_asset_name=data_asset_name,
                                     expectation_suite=expectation_suite,
                                     custom_sql=query,
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
