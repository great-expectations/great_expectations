import logging
import os

from great_expectations.datasource.types import SqlAlchemyDatasourceQueryBatchKwargs
from great_expectations.exceptions import BatchKwargsError, ClassInstantiationError

from ...data_context.util import instantiate_class_from_config
from .batch_kwargs_generator import BatchKwargsGenerator

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


class QueryBatchKwargsGenerator(BatchKwargsGenerator):
    """Produce query-style batch_kwargs from sql files stored on disk
    """

    recognized_batch_parameters = {"query_parameters", "partition_id", "name"}

    def __init__(
        self, name="default", datasource=None, query_store_backend=None, queries=None
    ):
        super(QueryBatchKwargsGenerator, self).__init__(
            name=name, datasource=datasource
        )
        root_directory = None
        if query_store_backend is None:
            # We will choose a Tuple store if there is a configured DataContext with a root_directory,
            # and an InMemoryStore otherwise
            if (
                datasource
                and datasource.data_context
                and datasource.data_context.root_directory
            ):
                query_store_backend = {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": os.path.join(
                        datasource.data_context.root_directory,
                        "datasources",
                        datasource.name,
                        "generators",
                        name,
                    ),
                    "filepath_suffix": ".sql",
                }
                root_directory = datasource.data_context.root_directory
            else:
                query_store_backend = {"class_name": "InMemoryStoreBackend"}
        module_name = "great_expectations.data_context.store"
        self._store_backend = instantiate_class_from_config(
            config=query_store_backend,
            runtime_environment={"root_directory": root_directory},
            config_defaults={"module_name": module_name},
        )
        if not self._store_backend:
            raise ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=query_store_backend["class_name"],
            )
        if queries is not None:
            for query_name, query in queries.items():
                self.add_query(query_name, query)

    def _get_raw_query(self, generator_asset):
        return self._store_backend.get((generator_asset,))

    def _get_iterator(self, generator_asset, query_parameters=None):
        raw_query = self._get_raw_query(generator_asset)
        if raw_query is None:
            logger.warning("No query defined for generator asset: %s" % generator_asset)
            # There is no valid query path or temp query storage defined with the generator_asset
            return None

        if query_parameters is None:
            iter_ = iter([SqlAlchemyDatasourceQueryBatchKwargs(query=raw_query)])
        else:
            iter_ = iter(
                [
                    SqlAlchemyDatasourceQueryBatchKwargs(
                        query=raw_query, query_parameters=query_parameters
                    )
                ]
            )

        return iter_

    def add_query(self, generator_asset, query):
        # Backends must have a tuple key; we use only a single-element tuple
        self._store_backend.set((generator_asset,), query)

    def get_available_data_asset_names(self):
        defined_queries = self._store_backend.list_keys()
        # Backends must have a tuple key; we use only a single-element tuple
        return {
            "names": [
                (query_key_tuple[0], "query") for query_key_tuple in defined_queries
            ]
        }

    def _build_batch_kwargs(self, batch_parameters):
        """Build batch kwargs from a partition id."""
        generator_asset = batch_parameters.pop("name")
        raw_query = self._get_raw_query(generator_asset)
        partition_id = batch_parameters.pop("partition_id", None)
        batch_kwargs = self._datasource.process_batch_parameters(**batch_parameters)
        batch_kwargs["query"] = raw_query

        if partition_id:
            if not batch_kwargs["query_parameters"]:
                batch_kwargs["query_parameters"] = {}
            batch_kwargs["query_parameters"]["partition_id"] = partition_id

        return SqlAlchemyDatasourceQueryBatchKwargs(batch_kwargs)

    def get_available_partition_ids(self, generator_asset):
        raise BatchKwargsError(
            "QueryBatchKwargsGenerator cannot identify partitions.", {}
        )
