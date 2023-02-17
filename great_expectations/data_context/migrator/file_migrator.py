from __future__ import annotations

import os
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


class FileMigrator:
    def migrate(
        self,
        source_context: AbstractDataContext,
        project_root_dir: PathStr = os.getcwd(),
    ) -> FileDataContext:
        if isinstance(source_context, FileDataContext):
            raise gx_exceptions.MigrationError(
                "Target context is already an instance of FileDataContext"
            )

        target_context = cast(
            FileDataContext, FileDataContext.create(project_root_dir=project_root_dir)
        )
        target_context = self._migrate_persisted_objects(
            target_context=target_context, source_context=source_context
        )

        return target_context

    def _migrate_persisted_objects(
        self,
        target_context: FileDataContext,
        source_context: AbstractDataContext,
    ) -> FileDataContext:
        source_stores = source_context.stores
        target_stores = target_context.stores

        if source_stores.keys() != target_stores.keys():
            raise gx_exceptions.MigrationError(
                "Cannot migrate context due to store configurations being out of sync"
            )

        for name in source_stores:
            source_store = source_stores[name]
            target_store = target_stores[name]

            for key in source_store.list_keys():
                source_obj = source_store.get(key)
                target_store.add(key=key, value=source_obj)
