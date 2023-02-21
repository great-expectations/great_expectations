from __future__ import annotations

import logging
import pathlib
import shutil
from typing import TYPE_CHECKING, cast

import great_expectations.exceptions.exceptions as gx_exceptions
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.types.base import DataContextConfigDefaults

if TYPE_CHECKING:
    from great_expectations.alias_types import PathStr
    from great_expectations.data_context.data_context_variables import (
        DataContextVariables,
    )
    from great_expectations.data_context.store.datasource_store import DatasourceStore
    from great_expectations.data_context.store.store import Store

logger = logging.getLogger(__name__)


class FileMigrator:
    """Encapsulates any logic necessary to convert an existing context to a FileDataContext

    Only takes in the necessary dependencies for conversion:
        - context.stores
        - context._datasource_store
        - context.variables
    """

    def __init__(
        self,
        primary_stores: dict[str, Store],
        datasource_store: DatasourceStore,
        variables: DataContextVariables,
    ) -> None:
        self._primary_stores = primary_stores
        self._datasource_store = datasource_store
        self._variables = variables

    def migrate(self, path: PathStr = pathlib.Path.cwd()) -> FileDataContext:
        """Migrate your in-memory Data Context to a file-backed one.

        Returns:
            A FileDataContext with an updated config to reflect the state of the current context.
        """
        dst_context = self._scaffold_filesystem(path)
        self._migrate_primary_stores(
            dst_stores=dst_context.stores,
        )
        self._migrate_datasource_store(dst_store=dst_context._datasource_store)
        self._migrate_data_docs_sites(
            dst_root=pathlib.Path(dst_context.root_directory),
            dst_variables=dst_context.variables,
        )

        # Re-init context to parse filesystem changes into config
        dst_context = FileDataContext()
        print(f"Successfully migrated to {dst_context.__class__.__name__}!")
        return dst_context

    def _scaffold_filesystem(self, path: PathStr) -> FileDataContext:
        if isinstance(path, str):
            path = pathlib.Path(path)

        path = path.absolute()
        if not path.exists():
            raise gx_exceptions.MigrationError(
                f"{path} does not exist; cannot migrate to a non-existent directory"
            )

        dst_context = cast(
            FileDataContext, FileDataContext.create(project_root_dir=str(path))
        )
        logger.info("Scaffolded necessary directories for a file-backed context")

        return dst_context

    def _migrate_primary_stores(self, dst_stores: dict[str, Store]) -> None:
        src_stores = self._primary_stores
        for name, src_store in src_stores.items():
            dst_store = dst_stores.get(name)
            if dst_store:
                self._migrate_store(
                    store_name=name, src_store=src_store, dst_store=dst_store
                )
            else:
                logger.warning(f"Could not migrate the contents of store {name}")

    def _migrate_datasource_store(self, dst_store: DatasourceStore) -> None:
        src_store = self._datasource_store
        self._migrate_store(
            store_name=DataContextConfigDefaults.DEFAULT_DATASOURCE_STORE_NAME.value,
            src_store=src_store,
            dst_store=dst_store,
        )

    def _migrate_store(
        self, store_name: str, src_store: Store, dst_store: Store
    ) -> None:
        logger.info(
            f"Migrating key-value pairs from {store_name} ({src_store.__class__})."
        )
        for key in src_store.list_keys():
            src_obj = src_store.get(key)
            dst_store.add(key=key, value=src_obj)
            logger.info(f"Successfully migrated stored object saved with key {key}.")

    def _migrate_data_docs_sites(
        self, dst_root: pathlib.Path, dst_variables: DataContextVariables
    ) -> None:
        src_configs = self._variables.data_docs_sites or {}

        dst_base_directory = dst_root.joinpath(
            DataContextConfigDefaults.DEFAULT_DATA_DOCS_BASE_DIRECTORY_RELATIVE_NAME.value
        )
        assert (
            dst_base_directory.exists()
        ), f"{dst_base_directory} should have been set up by upstream scaffolding"

        updated_data_docs_config = {}
        for site_name, site_config in src_configs.items():
            updated_site_config = self._migrate_data_docs_site(
                site_name=site_name,
                site_config=site_config,
                dst_base_directory=dst_base_directory,
            )
            updated_data_docs_config[site_name] = updated_site_config

        dst_variables.data_docs_sites = updated_data_docs_config
        dst_variables.save_config()

    def _migrate_data_docs_site(
        self, site_name: str, site_config: dict, dst_base_directory: pathlib.Path
    ) -> dict:
        store_backend = site_config["store_backend"]

        src_site = pathlib.Path(store_backend["base_directory"])
        dst_site = dst_base_directory.joinpath(site_name)

        if src_site.exists():
            shutil.copytree(src=str(src_site), dst=str(dst_site), dirs_exist_ok=True)
            logger.info(f"Migrated {site_name} from {src_site} to {dst_site}.")
        else:
            logger.info(
                f"{site_name} has never been built by the src context; skipping file copying."
            )

        updated_config = site_config
        site_path = dst_base_directory.joinpath(site_name)
        updated_config["store_backend"]["base_directory"] = str(site_path)

        return updated_config
