import os
import logging

from .batch_kwargs_generator import BatchKwargsGenerator
from great_expectations.datasource.types import SqlAlchemyDatasourceQueryBatchKwargs
from great_expectations.exceptions import BatchKwargsError
from ...data_context.util import instantiate_class_from_config

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


class QueryGenerator(BatchKwargsGenerator):
    """Produce query-style batch_kwargs from sql files stored on disk
    """

    def __init__(self, name="default", datasource=None, query_store_backend=None):
        super(QueryGenerator, self).__init__(name=name, datasource=datasource)
        root_directory = None
        if query_store_backend is None:
            # We will choose a Tuple store if there is a configured DataContext with a root_directory,
            # and an InMemoryStore otherwise
            if datasource and datasource.data_context and datasource.data_context.root_directory:
                query_store_backend = {
                    "class_name": "FileSystemTupleStoreBackend",
                    "base_directory": os.path.join(datasource.data_context.root_directory, "generators", name),
                    "filepath_suffix": ".sql"
                }
                root_directory = datasource.data_context.root_directory
            else:
                query_store_backend = {
                    "class_name": "InMemoryStoreBackend"
                }
        self._store_backend = instantiate_class_from_config(
            config=query_store_backend,
            runtime_environment={
                "root_directory": root_directory
            },
            config_defaults={
                "module_name": "great_expectations.data_context.store"
            }

        )

    def _get_raw_query(self, generator_asset):
        return self._store_backend.get(tuple(generator_asset))

    def _get_iterator(self, generator_asset, query_parameters=None):
        raw_query = self._get_raw_query(generator_asset)
        if raw_query is None:
            logger.warning("No query defined for generator asset: %s" % generator_asset)
            # There is no valid query path or temp query storage defined with the generator_asset
            return None

        if query_parameters is None:
            iter_ = iter([
                SqlAlchemyDatasourceQueryBatchKwargs(
                    query=raw_query
                )])
        else:
            iter_= iter([
                SqlAlchemyDatasourceQueryBatchKwargs(
                    query=raw_query,
                    query_params=query_parameters
                )])

        return iter_

    def add_query(self, generator_asset, query):
        self._store_backend.set(tuple(generator_asset), query)

    def get_available_data_asset_names(self):
        defined_queries = self._store_backend.list_keys()
        return [(query_name, "query") for query_name in defined_queries]

    def _build_batch_kwargs(self, batch_parameters):
        """Build batch kwargs from a partition id."""
        generator_asset = batch_parameters.get("name")
        raw_query = self._get_raw_query(generator_asset)
        batch_kwargs = {
            "query": raw_query
        }
        if "query_parameters" in batch_parameters:
            batch_kwargs["query_parameters"] = batch_parameters.get("query_parameters")

        return SqlAlchemyDatasourceQueryBatchKwargs(batch_kwargs)

    def get_available_partition_ids(self, generator_asset):
        raise BatchKwargsError("QueryGenerator cannot identify partitions.", {})
