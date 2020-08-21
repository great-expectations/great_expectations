import logging
from typing import Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context.store import Store, StoreBackend
from great_expectations.data_context.util import instantiate_class_from_config

logger = logging.getLogger(__name__)


def build_store_from_config(
    store_config: dict = None,
    module_name: str = "great_expectations.data_context.store",
    runtime_environment: dict = None,
) -> Union[StoreBackend, Store, None]:
    if store_config is None or module_name is None:
        return None

    try:
        config_defaults: dict = {"module_name": module_name}
        new_store: Union[StoreBackend, Store, None] = instantiate_class_from_config(
            config=store_config,
            runtime_environment=runtime_environment,
            config_defaults=config_defaults,
        )
    except ge_exceptions.DataContextError as e:
        new_store = None
        logger.critical(f"Error {e} occurred while attempting to instantiate a store.")
    if not new_store:
        class_name: str = store_config.get("class_name")
        raise ge_exceptions.ClassInstantiationError(
            module_name=module_name, package_name=None, class_name=class_name,
        )
    return new_store


def build_in_memory_store_backend(
    module_name: str = "great_expectations.data_context.store",
    class_name: str = "InMemoryStoreBackend",
    **kwargs,
) -> Union[StoreBackend, None]:
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
) -> Union[StoreBackend, None]:
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
) -> Union[StoreBackend, None]:
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
