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

if TYPE_CHECKING:
    from great_expectations.alias_types import PathStr
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.data_context.store.store import Store

logger = logging.getLogger(__name__)


class FileMigrator:
    def migrate(
        self,
        source_context: AbstractDataContext,
        project_root_dir: PathStr = os.getcwd(),
    ) -> FileDataContext:
        if isinstance(source_context, FileDataContext):
            raise gx_exceptions.MigrationError(
                f"Context is already an instance of {FileDataContext.__name__}; cannot migrate."
            )

        converted_context = cast(
            FileDataContext, FileDataContext.create(project_root_dir=project_root_dir)
        )
        self._migrate_store_contents(
            source_stores=source_context.stores,
            destination_stores=converted_context.stores,
        )
        self._migrate_data_docs_sites(
            source_context=source_context, destination_context=converted_context
        )

        return converted_context

    def _migrate_store_contents(
        self,
        source_stores: dict[str, Store],
        destination_stores: dict[str, Store],
    ) -> None:
        if source_stores.keys() != destination_stores.keys():
            raise gx_exceptions.MigrationError(
                "Cannot migrate context due to store configurations being out of sync."
            )

        for name in source_stores:
            source_store = source_stores[name]
            target_store = destination_stores[name]
            logger.info(
                f"Migrating key-value pairs from {name} ({source_store.__class__})."
            )
            for key in source_store.list_keys():
                source_obj = source_store.get(key)
                target_store.add(key=key, value=source_obj)
                logger.info(
                    f"Successfully migrated stored object saved with key {key}."
                )

    def _migrate_data_docs_sites(
        self,
        source_context: AbstractDataContext,
        destination_context: FileDataContext,
    ):
        source_configs = source_context.variables.data_docs_sites or {}

        destination_root = pathlib.Path(destination_context.root_directory)
        destination_base_directory = destination_root.joinpath("uncommitted/data_docs")

        for site_name, site_config in source_configs.items():
            store_backend = site_config["store_backend"]
            source_base_directory = pathlib.Path(store_backend["base_directory"])

            source_site = source_base_directory.joinpath(site_name)
            destination_site = destination_base_directory.joinpath(site_name)

            destination_site.mkdir()
            shutil.move(source_site, destination_site)
            logger.info(
                f"Migrated {site_name} from {source_site} to {destination_site}."
            )
