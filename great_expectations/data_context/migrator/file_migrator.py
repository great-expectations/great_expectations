from __future__ import annotations

import logging
import pathlib
from typing import TYPE_CHECKING, cast

from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.data_context.serializable_data_context import (
    SerializableDataContext,
)
from great_expectations.data_context.types.base import DataContextConfigDefaults

if TYPE_CHECKING:
    from great_expectations.data_context.data_context_variables import (
        DataContextVariables,
    )
    from great_expectations.data_context.store.datasource_store import DatasourceStore
    from great_expectations.data_context.store.store import Store
    from great_expectations.datasource.fluent.config import GxConfig

logger = logging.getLogger(__name__)


class FileMigrator:
    """Encapsulates any logic necessary to convert an existing context to a FileDataContext

    Only takes in the necessary dependencies for conversion:
        - context.stores
        - context._datasource_store
        - context.variables
        - context.fluent_config
    """

    def __init__(
        self,
        primary_stores: dict[str, Store],
        datasource_store: DatasourceStore,
        variables: DataContextVariables,
        fluent_config: GxConfig,
    ) -> None:
        self._primary_stores = primary_stores
        self._datasource_store = datasource_store
        self._variables = variables
        self._fluent_config = fluent_config

    def migrate(self) -> FileDataContext:
        """Migrate your in-memory Data Context to a file-backed one.

        Takes the following steps:
            1. Scaffolds filesystem
            2. Migrates primary stores (only creates default named stores)
            3. Migrates datasource store
            4. Migrates data docs sites (both physical files and config)
            5. Migrates fluent datasources

        Returns:
            A FileDataContext with an updated config to reflect the state of the current context.
        """
        target_context = self._scaffold_filesystem()
        self._migrate_primary_stores(
            target_stores=target_context.stores,
        )
        self._migrate_datasource_store(target_store=target_context._datasource_store)
        self._migrate_data_docs_sites(
            target_context=target_context,
        )
        self._migrate_fluent_datasources(target_context=target_context)

        # Re-init context to parse filesystem changes into config
        target_context = FileDataContext()
        print(f"Successfully migrated to {target_context.__class__.__name__}!")
        return target_context

    def _scaffold_filesystem(self) -> FileDataContext:
        path = pathlib.Path.cwd().absolute()
        target_context = cast(
            FileDataContext, FileDataContext.create(project_root_dir=str(path))
        )
        logger.info("Scaffolded necessary directories for a file-backed context")

        return target_context

    def _migrate_primary_stores(self, target_stores: dict[str, Store]) -> None:
        source_stores = self._primary_stores
        for name, source_store in source_stores.items():
            target_store = target_stores.get(name)
            if target_store:
                self._migrate_store(
                    store_name=name,
                    source_store=source_store,
                    target_store=target_store,
                )
            else:
                logger.warning(
                    f"Could not migrate the contents of store {name}; only default named stores are migrated"
                )

    def _migrate_datasource_store(self, target_store: DatasourceStore) -> None:
        source_store = self._datasource_store
        self._migrate_store(
            store_name=DataContextConfigDefaults.DEFAULT_DATASOURCE_STORE_NAME.value,
            source_store=source_store,
            target_store=target_store,
        )

    def _migrate_store(
        self, store_name: str, source_store: Store, target_store: Store
    ) -> None:
        logger.info(
            f"Migrating key-value pairs from {store_name} ({source_store.__class__})."
        )
        for key in source_store.list_keys():
            source_obj = source_store.get(key)
            target_store.add(key=key, value=source_obj)
            logger.info(f"Successfully migrated stored object saved with key {key}.")

    def _migrate_data_docs_sites(self, target_context: FileDataContext) -> None:
        target_root = pathlib.Path(target_context.root_directory)
        target_variables = target_context.variables
        source_configs = self._variables.data_docs_sites or {}

        self._migrate_data_docs_site_configs(
            target_root=target_root,
            source_configs=source_configs,
            target_variables=target_variables,
        )
        target_context.build_data_docs()

    def _migrate_fluent_datasources(self, target_context: FileDataContext) -> None:
        target_context.fluent_config = self._fluent_config
        target_context._save_project_config()

    def _migrate_data_docs_site_configs(
        self,
        source_configs: dict,
        target_root: pathlib.Path,
        target_variables: DataContextVariables,
    ):
        target_base_directory = target_root.joinpath(
            DataContextConfigDefaults.DEFAULT_DATA_DOCS_BASE_DIRECTORY_RELATIVE_NAME.value
        )

        updated_data_docs_config = {}
        for site_name, site_config in source_configs.items():
            updated_site_config = self._migrate_data_docs_site_config(
                site_name=site_name,
                site_config=site_config,
                target_base_directory=target_base_directory,
            )
            updated_data_docs_config[site_name] = updated_site_config

        # If no sites to migrate, don't touch config defaults
        if updated_data_docs_config:
            target_variables.data_docs_sites = updated_data_docs_config
            target_variables.save_config()

    def _migrate_data_docs_site_config(
        self, site_name: str, site_config: dict, target_base_directory: pathlib.Path
    ) -> dict:
        absolute_site_path = target_base_directory.joinpath(site_name)
        project_root = pathlib.Path.cwd().joinpath(SerializableDataContext.GX_DIR)
        relative_site_path = absolute_site_path.relative_to(project_root)

        updated_config = site_config
        updated_config["store_backend"]["base_directory"] = str(relative_site_path)

        return updated_config
