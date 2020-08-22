import logging

from great_expectations.data_context.util import build_store_from_config

logger = logging.getLogger(__name__)


def build_in_memory_store_backend(
    module_name: str = "great_expectations.data_context.store",
    class_name: str = "InMemoryStoreBackend",
    **kwargs,
):
    logger.debug(f"Starting data_context/store/util.py#build_in_memory_store_backend")
    store_config: dict = {"module_name": module_name, "class_name": class_name}
    store_config.update(**kwargs)
    return build_store_from_config(
        store_config=store_config, module_name=module_name, runtime_environment=None,
    )


def build_tuple_filesystem_store_backend(
    base_directory: str,
    *,
    module_name: str = "great_expectations.data_context.store",
    class_name: str = "TupleFilesystemStoreBackend",
    **kwargs,
):
    logger.debug(
        f"Starting data_context/store/util.py#build_tuple_filesystem_store_backend"
    )
    store_config: dict = {
        "module_name": module_name,
        "class_name": class_name,
        "base_directory": base_directory,
    }
    store_config.update(**kwargs)
    return build_store_from_config(
        store_config=store_config, module_name=module_name, runtime_environment=None,
    )


def build_tuple_s3_store_backend(
    bucket: str,
    *,
    module_name: str = "great_expectations.data_context.store",
    class_name: str = "TupleS3StoreBackend",
    **kwargs,
):
    logger.debug(f"Starting data_context/store/util.py#build_tuple_s3_store_backend")
    store_config: dict = {
        "module_name": module_name,
        "class_name": class_name,
        "bucket": bucket,
    }
    store_config.update(**kwargs)
    return build_store_from_config(
        store_config=store_config, module_name=module_name, runtime_environment=None,
    )


def build_configuration_store(
    store_name: str,
    store_backend,
    *,
    module_name: str = "great_expectations.data_context.store",
    class_name: str = "ConfigurationStore",
    **kwargs,
):
    logger.debug(
        f"Starting data_context/store/util.py#build_configuration_store for store_name {store_name}"
    )
    if store_backend is not None:
        store_backend = store_backend.config
    store_config: dict = {
        "store_name": store_name,
        "module_name": module_name,
        "class_name": class_name,
        "store_backend": store_backend,
    }
    store_config.update(**kwargs)
    # noinspection PyTypeChecker
    configuration_store = build_store_from_config(
        store_config=store_config, module_name=module_name, runtime_environment=None,
    )
    return configuration_store
