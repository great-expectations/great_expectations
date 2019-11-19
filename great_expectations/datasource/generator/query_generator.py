import os
import logging
from string import Template

from .batch_generator import BatchGenerator
from great_expectations.datasource.types import SqlAlchemyDatasourceQueryBatchKwargs
from great_expectations.exceptions import BatchKwargsError

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

    # FIXME: This needs to be updated to use a store so that the query generator does not have to manage storage itself
    # FIXME: New tests should then be added
    def __init__(self, name="default", datasource=None, queries=None):
        super(QueryGenerator, self).__init__(name=name, datasource=datasource)
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

        if queries is None:
            queries = {}

        self._queries = queries

    def _get_raw_query(self, generator_asset):
        raw_query = None
        if self._queries_path:
            if generator_asset in [path[:-4] for path in os.listdir(self._queries_path) if str(path).endswith(".sql")]:
                with open(os.path.join(self._queries_path, generator_asset) + ".sql", "r") as data:
                    raw_query = data.read()
        elif self._queries:
            if generator_asset in self._queries:
                raw_query = self._queries[generator_asset]

        return raw_query

    def _get_iterator(self, generator_asset, query_params=None):
        raw_query = self._get_raw_query(generator_asset)
        if raw_query is None:
            logger.warning("No query defined for generator asset: %s" % generator_asset)
            # There is no valid query path or temp query storage defined with the generator_asset
            return None

        if query_params is None:
            query_params = {}
        try:
            substituted_query = Template(raw_query).substitute(query_params)
        except KeyError:
            raise BatchKwargsError(
                "Unable to generate batch kwargs for asset '" + generator_asset + "': "
                "missing template key",
                {
                    "generator_asset": generator_asset,
                    "query_template": raw_query
                }
            )
        return iter([
            SqlAlchemyDatasourceQueryBatchKwargs(
                query=substituted_query,
                raw_query=raw_query,
                query_params=query_params
            )])

    def add_query(self, generator_asset, query):
        if self._queries_path:
            with open(os.path.join(self._queries_path, generator_asset + ".sql"), "w") as queryfile:
                queryfile.write(query)
        else:
            logger.info("Adding query to temporary storage only.")
            self._queries[generator_asset] = query

    def get_available_data_asset_names(self):
        if self._queries_path:
            defined_queries = [path for path in os.walk(self._queries_path) if str(path).endswith(".sql")]
        else:
            defined_queries = list(self._queries.keys())

        return defined_queries

    def build_batch_kwargs_from_partition_id(self, generator_asset, partition_id=None, query_params=None):
        """Build batch kwargs from a partition id."""
        raw_query = self._get_raw_query(generator_asset)
        if "$partition_id" not in raw_query and "${partition_id}" not in raw_query:
            raise BatchKwargsError("No partition_id parameter found in the requested query.", {})
        try:
            if query_params is None:
                query_params = {}
            query_params.update({'partition_id': partition_id})
            substituted_query = Template(raw_query).substitute(query_params)
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
            query=substituted_query,
            raw_query=raw_query,
            query_params=query_params
        )

    def get_available_partition_ids(self, generator_asset):
        raise BatchKwargsError("QueryGenerator cannot identify partitions, however any asset defined with"
                               "a single parameter can be accessed using that parameter as a partition_id.", {})
