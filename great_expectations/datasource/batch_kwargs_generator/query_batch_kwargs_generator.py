import logging
import os
import warnings

from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.batch_kwargs_generator.batch_kwargs_generator import (
    BatchKwargsGenerator,
)
from great_expectations.datasource.types import SqlAlchemyDatasourceQueryBatchKwargs
from great_expectations.exceptions import BatchKwargsError, ClassInstantiationError

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
    'Produce query-style batch_kwargs from sql files or defined queries.\n\n    By default, a QueryBatchKwargsGenerator will look for queries in the\n    ``datasources/datasource_name/generators/generator_name`` directory, and look for files ending in ``.sql``.\n\n    For example, a file stored in\n    ``datasources/datasource_name/generators/generator_name/movies_by_date.sql`` would allow you to access an\n    asset called ``movies_by_date``\n\n    Queries can be parameterized using $substitution.\n\n    Example configuration:\n\n      queries:\n        class_name: QueryBatchKwargsGenerator\n        query_store_backend:\n          class_name: TupleFilesystemStoreBackend\n          filepath_suffix: .sql\n          base_directory: queries\n\n    Example query template, to be stored in ``queries/movies_by_date.sql``\n\n    SELECT * FROM movies where \'$start\'::date <= release_date AND release_date <= \'$end\'::date;\n\n    Example usage:\n\n    context.build_batch_kwargs(\n        "my_db",\n        "query_generator",\n        "movies_by_date",\n        "query_parameters": {\n            "start": "2020-01-01",\n            "end": "2020-02-01"\n        }\n'
    recognized_batch_parameters = {
        "query_parameters",
        "partition_id",
        "data_asset_name",
    }

    def __init__(
        self, name="default", datasource=None, query_store_backend=None, queries=None
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        super().__init__(name=name, datasource=datasource)
        if (
            datasource
            and datasource.data_context
            and datasource.data_context.root_directory
        ):
            root_directory = datasource.data_context.root_directory
        else:
            root_directory = None
        if query_store_backend is None:
            if root_directory:
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
            for (query_name, query) in queries.items():
                self.add_query(data_asset_name=query_name, query=query)

    def _get_raw_query(self, data_asset_name):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._store_backend.get((data_asset_name,))

    def _get_iterator(self, data_asset_name, query_parameters=None):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        raw_query = self._get_raw_query(data_asset_name=data_asset_name)
        if raw_query is None:
            logger.warning(f"No query defined for data asset: {data_asset_name}")
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

    def add_query(self, generator_asset=None, query=None, data_asset_name=None) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        assert query, "Please provide a query."
        assert (generator_asset and (not data_asset_name)) or (
            (not generator_asset) and data_asset_name
        ), "Please provide either generator_asset or data_asset_name."
        if generator_asset:
            warnings.warn(
                "The 'generator_asset' argument is deprecated as of v0.11.0 and will be removed in v0.16. Please use 'data_asset_name' instead.",
                DeprecationWarning,
            )
            data_asset_name = generator_asset
        self._store_backend.set((data_asset_name,), query)

    def get_available_data_asset_names(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        defined_queries = self._store_backend.list_keys()
        return {
            "names": [
                (query_key_tuple[0], "query") for query_key_tuple in defined_queries
            ]
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
        "Build batch kwargs from a partition id."
        data_asset_name = batch_parameters.pop("data_asset_name")
        raw_query = self._get_raw_query(data_asset_name=data_asset_name)
        partition_id = batch_parameters.pop("partition_id", None)
        batch_kwargs = self._datasource.process_batch_parameters(**batch_parameters)
        batch_kwargs["query"] = raw_query
        if partition_id:
            if not batch_kwargs["query_parameters"]:
                batch_kwargs["query_parameters"] = {}
            batch_kwargs["query_parameters"]["partition_id"] = partition_id
        return SqlAlchemyDatasourceQueryBatchKwargs(batch_kwargs)

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
            "QueryBatchKwargsGenerator cannot identify partitions.", {}
        )
