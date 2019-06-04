import time
import os
import logging
from string import Template

import sqlalchemy
from sqlalchemy import create_engine, MetaData

from .datasource import Datasource
from .batch_generator import BatchGenerator
from ...dataset.sqlalchemy_dataset import SqlAlchemyDataset

logger = logging.getLogger(__name__)

class QueryGenerator(BatchGenerator):
    """
    """

    def __init__(self, name="default", datasource=None, engine=None):
        super(QueryGenerator, self).__init__(name=name, type_="queries", datasource=datasource)
        self.meta = MetaData()
        if datasource is not None and datasource._data_context is not None:
            self._queries_path = os.path.join(self._datasource._data_context.context_root_directory, 
                "great_expectations/datasources", 
                self._datasource._name,
                "generators",
                self._name,
                "queries")
        else:
            self._queries_path = None
            self._queries = {}

        if datasource is not None:
            self.engine = datasource.engine

    def _get_iterator(self, data_asset_name):
        if self._queries_path:
            if data_asset_name in [path for path in os.walk(self._queries_path) if path.endswith(".sql")]:
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

    def list_available_data_asset_names(self):
        if self._queries_path:
            defined_queries = [path for path in os.walk(self._queries_path) if path.endswith(".sql")]
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
        uses $parameter, with additional kwargs passed to the get_data_asset method.
    """

    def __init__(self, name="default", data_context=None, profile=None, generators=None, **kwargs):
        if generators is None:
            generators = {
                "default": {"type": "queries"}
        }
        super(SqlAlchemyDatasource, self).__init__(name, type_="sqlalchemy", data_context=data_context, generators=generators)
        if profile is not None:
            self._datasource_config.update({
                "profile": profile
            })
            credentials = data_context.get_profile_credentials(profile)
        else:
            credentials = {}

        # if an engine was provided, use that
        kwarg_engine = kwargs.pop("engine", None)
        if kwarg_engine is not None:
            self.engine = kwarg_engine
        else:
        
            # Update credentials with anything passed during connection time
            credentials.update(dict(**kwargs))
            self.meta = MetaData()

            if "url" in credentials:
                options = credentials.pop("url")
            else:
                drivername = credentials.pop("drivername")
                options = sqlalchemy.engine.url.URL(drivername, **credentials)
            self._connect(options)

        self._build_generators()

    def _connect(self, options, *args, **kwargs):
        self.engine = create_engine(options, *args, **kwargs)

    def _get_generator_class(self, type_):
        if type_ == "queries":
            return QueryGenerator
        else:
            raise ValueError("Unrecognized DataAssetGenerator type %s" % type_)

    def _get_data_asset(self, data_asset_name, batch_kwargs, expectations_config, schema=None, **kwargs):
        if "table" in batch_kwargs:
            return SqlAlchemyDataset(table_name=batch_kwargs["table"], 
                engine=self.engine,
                schema=schema,
                data_context=self._data_context, 
                data_asset_name=data_asset_name, 
                expectations_config=expectations_config, 
                batch_kwargs=batch_kwargs)        

        elif "query" in batch_kwargs:
            query = Template(batch_kwargs["query"]).safe_substitute(**kwargs)
            return SqlAlchemyDataset(table_name=data_asset_name, 
                engine=self.engine,
                data_context=self._data_context, 
                data_asset_name=data_asset_name, 
                expectations_config=expectations_config, 
                custom_sql=query, 
                batch_kwargs=batch_kwargs)
    
        else:
            raise ValueError("Invalid batch_kwargs: exactly one of 'table' or 'query' must be specified")

    def build_batch_kwargs(self, table=None, query=None):
        if (table is None and query is None) or (table is not None and query is not None):
            raise ValueError("Exactly one of 'table' or 'query' must be specified.")

        if table is not None:
            return {
                "table": table,
                "timestamp": time.time()
            }
        else:
            return {
                "query": query,
                "timestamp": time.time()
            }
