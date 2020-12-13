from typing import Any, Optional, cast

import logging

from great_expectations.data_context.types.base import (
    BaseConfig,
    CheckpointConfig,
)
from great_expectations.data_context.store import StoreBackend
from great_expectations.data_context.util import build_store_from_config
import great_expectations.exceptions as ge_exceptions

logger = logging.getLogger(__name__)


def build_in_memory_store_backend(
    module_name: str = "great_expectations.data_context.store",
    class_name: str = "InMemoryStoreBackend",
    **kwargs,
):
    logger.debug("Starting data_context/store/util.py#build_in_memory_store_backend")
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
        f"""Starting data_context/store/util.py#build_tuple_filesystem_store_backend using base_directory:
"{base_directory}"""
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
    logger.debug(
        f"""Starting data_context/store/util.py#build_tuple_s3_store_backend using bucket: {bucket}
        """
    )
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
    configuration_class: Any,
    store_name: str,
    store_backend,
    *,
    module_name: str = "great_expectations.data_context.store",
    class_name: str = "ConfigurationStore",
    overwrite_existing: bool = False,
    **kwargs,
):
    logger.debug(
        f"Starting data_context/store/util.py#build_configuration_store for store_name {store_name}"
    )
    if store_backend is not None and issubclass(type(store_backend), StoreBackend):
        store_backend = store_backend.config
    elif not isinstance(store_backend, dict):
        raise ge_exceptions.DataContextError(
            "Invalid configuration: A store_backend needs to be a dictionary or inherit from the StoreBackend class."
        )
    store_config: dict = {
        "configuration_class": configuration_class,
        "store_name": store_name,
        "module_name": module_name,
        "class_name": class_name,
        "overwrite_existing": overwrite_existing,
        "store_backend": store_backend,
    }
    store_config.update(**kwargs)
    # noinspection PyTypeChecker
    configuration_store = build_store_from_config(
        store_config=store_config, module_name=module_name, runtime_environment=None,
    )
    return configuration_store


def save_config_to_filesystem(
    configuration_class: Any,
    store_name: str,
    base_directory: str,
    config: BaseConfig,
):
    store_config: dict = {
        "base_directory": base_directory
    }
    store_backend_obj = build_tuple_filesystem_store_backend(**store_config)
    config_store = build_configuration_store(
        configuration_class=configuration_class,
        store_name=store_name,
        store_backend=store_backend_obj,
        overwrite_existing=True,
    )
    config_store.save_configuration(configuration=config)


def load_config_from_filesystem(
    configuration_class: Any,
    store_name: str,
    base_directory: str,
) -> BaseConfig:
    store_config: dict = {
        "base_directory": base_directory
    }
    store_backend_obj = build_tuple_filesystem_store_backend(**store_config)
    config_store = build_configuration_store(
        configuration_class=configuration_class,
        store_name=store_name,
        store_backend=store_backend_obj,
        overwrite_existing=False,
    )

    config: Optional[configuration_class]
    try:
        # noinspection PyTypeChecker
        config = config_store.load_configuration()
        return config
    except (ge_exceptions.ConfigNotFoundError, ge_exceptions.InvalidBaseConfigError) as exc:
        logger.error(exc.messages)
        raise ge_exceptions.InvalidBaseConfigError(
            "Error while processing DataContextConfig.",
            exc
        )


def delete_config_from_filesystem(
    configuration_class: Any,
    store_name: str,
    base_directory: str,
):
    store_config: dict = {
        "base_directory": base_directory
    }
    store_backend_obj = build_tuple_filesystem_store_backend(**store_config)
    config_store = build_configuration_store(
        configuration_class=configuration_class,
        store_name=store_name,
        store_backend=store_backend_obj,
        overwrite_existing=True,
    )
    config_store.delete_configuration()


def save_checkpoint_config_to_filesystem(
    store_name: str,
    base_directory: str,
    checkpoint_config: CheckpointConfig,
):
    save_config_to_filesystem(
        configuration_class=CheckpointConfig,
        store_name=store_name,
        base_directory=base_directory,
        config=checkpoint_config
    )


def load_checkpoint_config_from_filesystem(
    store_name: str,
    base_directory: str,
) -> CheckpointConfig:
    try:
        checkpoint_config: CheckpointConfig = cast(
            CheckpointConfig,
            load_config_from_filesystem(
                configuration_class=CheckpointConfig,
                store_name=store_name,
                base_directory=base_directory,
            )
        )
        return checkpoint_config
    except ge_exceptions.InvalidBaseConfigError as exc:
        logger.error(exc.messages)
        raise ge_exceptions.InvalidCheckpointConfigError(
            "Error while processing DataContextConfig.",
            exc
        )


def delete_checkpoint_config_from_filesystem(
    store_name: str,
    base_directory: str,
):
    delete_config_from_filesystem(
        configuration_class=CheckpointConfig,
        store_name=store_name,
        base_directory=base_directory,
    )
