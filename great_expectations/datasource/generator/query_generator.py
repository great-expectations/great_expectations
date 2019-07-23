import os
import logging
import time

from .batch_generator import BatchGenerator

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


class QueryGenerator(BatchGenerator):
    """Produce query-style batch_kwargs from sql files stored on disk
    """

    def __init__(self, datasource, name="default"):
        # TODO: Add tests for QueryGenerator
        super(QueryGenerator, self).__init__(name=name, type_="queries", datasource=datasource)
        if (
                datasource is not None and
                datasource.data_context is not None and
                os.path.isdir(os.path.join(self._datasource.data_context.root_directory,
                                           "datasources",
                                           self._datasource.name,
                                           "generators",
                                           self._name,
                                           "queries")
                              )
        ):
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
            try:
                self.inspector = sqlalchemy.inspect(self.engine)

            except sqlalchemy.exc.OperationalError:
                logger.warning("Unable to create inspector from engine in generator '%s'" % name)
                self.inspector = None

    def _get_iterator(self, data_asset_name, **kwargs):
        if self._queries_path:
            if data_asset_name in [path[:-4] for path in os.listdir(self._queries_path) if str(path).endswith(".sql")]:
                with open(os.path.join(self._queries_path, data_asset_name) + ".sql", "r") as data:
                    return iter([{
                        "query": data.read(),
                        "timestamp": time.time()
                    }])
        elif self._queries:
            if data_asset_name in self._queries:
                return iter([{
                    "query": self._queries[data_asset_name],
                    "timestamp": time.time()
                }])
        else:
            # There is no query path or temp query storage defined
            pass

        if self.engine is not None and self.inspector is not None:
            split_data_asset_name = data_asset_name.split(".")
            if len(split_data_asset_name) == 2:
                schema_name = split_data_asset_name[0]
                table_name = split_data_asset_name[1]
            elif len(split_data_asset_name) == 1:
                schema_name = self.inspector.default_schema_name
                table_name = split_data_asset_name[0]
            else:
                raise ValueError("Table name must be of shape '[SCHEMA.]TABLE'. Passed: " + data_asset_name)
            tables = self.inspector.get_table_names(schema=schema_name)
            if table_name in tables:
                return iter([
                    {
                        "table": table_name,
                        "schema": schema_name,
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

        tables = []
        if self.engine is not None and self.inspector is not None:
            for schema_name in self.inspector.get_schema_names():
                #FIXME: create a list of names of info schemas for diff engines supported by sqlalchemy
                if schema_name in ['information_schema']:
                    continue

                tables.extend([table_name if self.inspector.default_schema_name == schema_name else schema_name + "." + table_name for table_name in self.inspector.get_table_names(schema=schema_name)])

        return set(defined_queries + tables)
