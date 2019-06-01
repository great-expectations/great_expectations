import datetime
import os
from string import Template

import sqlalchemy
from sqlalchemy import create_engine, MetaData

from .datasource import Datasource
from .batch_generator import BatchGenerator
from ...dataset.sqlalchemy_dataset import SqlAlchemyDataset

class QueryGenerator(BatchGenerator):
    """
    """

    def __init__(self, name, type_, datasource):
        super(QueryGenerator, self).__init__(name, type_, datasource)
        # TODO: Implement table-level introspection
        self.meta = MetaData()
        self._queries_path = os.path.join(self._datasource._data_context.context_root_directory, 
                "great_expectations/datasources", 
                self._datasource._name,
                "generators",
                self._name,
                "queries")

    def _get_iterator(self, data_asset_name):
        if data_asset_name in [path for path in os.walk(self._queries_path) if path.endswith(".sql")]:
            with open(os.path.join(self._queries_path, data_asset_name) + ".sql", "r") as data:
                return iter([{
                    "query": data.read(),
                    "timestamp": datetime.datetime.now().timestamp()
                }])

        self.meta.reflect(bind=self._datasource.engine)
        tables = [str(table) for table in self.meta.sorted_tables]
        if data_asset_name in tables:
            return iter([
                {
                    "table": data_asset_name,
                    "timestamp": datetime.datetime.now().timestamp()
                }
            ])

    def add_query(self, data_asset_name, query):
        with open(os.path.join(self._queries_path, data_asset_name + ".sql"), "w") as queryfile:
            queryfile.write(query)

    def list_data_asset_names(self):
        defined_queries = [path for path in os.walk(self._queries_path) if path.endswith(".sql")]
        self.meta.reflect(bind=self._datasource.engine)
        tables = [str(table) for table in self.meta.sorted_tables]
        return defined_queries + tables

class SqlAlchemyDatasource(Datasource):
    """
    A SqlAlchemyDatasource will provide data_assets converting batch_kwargs using the following rules:
      - if the batch_kwargs include a table key, the datasource will provide a dataset object connected
        to that table
      - if the batch_kwargs include a query key, the datasource will create a temporary table using that
        that query. The query can be parameterized according to the standard python Template engine, which
        uses $parameter, with additional kwargs passed to the get_data_asset method.
    """

    def __init__(self, name="default", data_context=None, profile_name=None, generators=None, **kwargs):
        if generators is None:
            generators = {
                "default": {"type": "queries"}
        }
        super(SqlAlchemyDatasource, self).__init__(name, type_="sqlalchemy", data_context=data_context, generators=generators)
        if profile_name is not None:
            self._datasource_config.update({
                "profile": profile_name
            })
            credentials = data_context.get_profile_credentials(profile_name)
        else:
            credentials = {}
        
        # Update credentials with anything passed during connection time
        credentials.update({**kwargs})
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

    def _get_data_asset_generator_class(self, type_):
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
                "timestamp": datetime.datetime.now().timestamp()
            }
        else:
            return {
                "query": query,
                "timestamp": datetime.datetime.now().timestamp()
            }
