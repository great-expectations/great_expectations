from __future__ import annotations

import logging
import os
import pathlib
import shutil
from typing import TYPE_CHECKING, cast

import great_expectations.exceptions.exceptions as gx_exceptions
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.types.base import DataContextConfigDefaults

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.data_context.store.datasource_store import DatasourceStore
    from great_expectations.data_context.store.store import Store

logger = logging.getLogger(__name__)


class FileMigrator:
    def __init__(self, context: AbstractDataContext) -> None:
        if isinstance(context, FileDataContext):
            raise gx_exceptions.MigrationError(
                f"Context is already an instance of {FileDataContext.__name__}; cannot migrate."
            )
        self._src_context = context

    def migrate(
        self,
    ) -> FileDataContext:
        pwd = os.getcwd()
        dst_context = cast(
            FileDataContext, FileDataContext.create(project_root_dir=pwd)
        )
        logger.info("Scaffolded necessary directories for a file-backed context")

        self._migrate_primary_stores(
            dst_stores=dst_context.stores,
        )
        self._migrate_datasource_store(dst_store=dst_context._datasource_store)
        self._migrate_data_docs_sites(dst_root=pathlib.Path(dst_context.root_directory))

        # Re-init context to parse filesystem changes into config
        dst_context = FileDataContext()
        print(
            f"Successfully migrated {self._src_context.__class__.__name__} to {dst_context.__class__.__name__}!"
        )
        return dst_context

    def _migrate_primary_stores(
        self,
        dst_stores: dict[str, Store],
    ) -> None:
        src_stores = self._src_context.stores
        if src_stores.keys() != dst_stores.keys():
            raise gx_exceptions.MigrationError(
                "Cannot migrate context due to store configurations being out of sync."
            )

        for name in src_stores:
            src_store = src_stores[name]
            dst_store = dst_stores[name]
            self._migrate_store(
                store_name=name, src_store=src_store, dst_store=dst_store
            )

    def _migrate_datasource_store(self, dst_store: DatasourceStore) -> None:
        src_store = self._src_context._datasource_store
        self._migrate_store(
            store_name="datasource name", src_store=src_store, dst_store=dst_store
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
        self,
        dst_root: pathlib.Path,
    ) -> None:
        src_configs = self._src_context.variables.data_docs_sites or {}

        # dst_root = pathlib.Path(dst_context.root_directory)
        dst_base_directory = dst_root.joinpath(
            DataContextConfigDefaults.DEFAULT_DATA_DOCS_BASE_DIRECTORY_RELATIVE_NAME.value
        )
        assert (
            dst_base_directory.exists()
        ), f"{dst_base_directory} should have been set up by upstream scaffolding"

        for site_name, site_config in src_configs.items():
            self._migrate_data_docs_site(
                site_name=site_name,
                site_config=site_config,
                dst_base_directory=dst_base_directory,
            )

    def _migrate_data_docs_site(
        self, site_name: str, site_config: dict, dst_base_directory: pathlib.Path
    ) -> None:
        store_backend = site_config["store_backend"]

        src_site = pathlib.Path(store_backend["base_directory"])
        dst_site = dst_base_directory.joinpath(site_name)

        if src_site.exists():
            shutil.copytree(src=str(src_site), dst=str(dst_site))
            logger.info(f"Migrated {site_name} from {src_site} to {dst_site}.")
        else:
            logger.info(
                f"{site_name} has never been built by the src context; skipping file copying."
            )
