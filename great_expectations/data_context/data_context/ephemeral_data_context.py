from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, Mapping, Optional, Union, cast

from great_expectations.core._docs_decorators import public_api
from great_expectations.core.serializer import DictConfigSerializer
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context_variables import (
    EphemeralDataContextVariables,
)
from great_expectations.data_context.migrator.file_migrator import FileMigrator
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DatasourceConfig,
    datasourceConfigSchema,
)

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.file_data_context import (
        FileDataContext,
    )
    from great_expectations.data_context.store.datasource_store import DatasourceStore

logger = logging.getLogger(__name__)


@public_api
class EphemeralDataContext(AbstractDataContext):
    """Subclass of AbstractDataContext that uses runtime values to generate a temporary or in-memory DataContext."""

    def __init__(
        self,
        project_config: Union[DataContextConfig, Mapping],
        runtime_environment: Optional[dict] = None,
    ) -> None:
        """EphemeralDataContext constructor

        project_config: config for in-memory EphemeralDataContext
        runtime_environment: a dictionary of config variables tha
                override both those set in config_variables.yml and the environment

        """
        self._project_config = self._init_project_config(project_config)
        super().__init__(runtime_environment=runtime_environment)

    def _init_project_config(
        self, project_config: Union[DataContextConfig, Mapping]
    ) -> DataContextConfig:
        project_config = EphemeralDataContext.get_or_create_data_context_config(
            project_config
        )
        return self._apply_global_config_overrides(project_config)

    def _init_variables(self) -> EphemeralDataContextVariables:
        variables = EphemeralDataContextVariables(
            config=self._project_config,
            config_provider=self.config_provider,
        )
        return variables

    def _init_datasource_store(self) -> DatasourceStore:
        from great_expectations.data_context.store.datasource_store import (
            DatasourceStore,
        )

        store_name: str = "datasource_store"  # Never explicitly referenced but adheres
        # to the convention set by other internal Stores
        store_backend: dict = {"class_name": "InMemoryStoreBackend"}

        datasource_store = DatasourceStore(
            store_name=store_name,
            store_backend=store_backend,
            serializer=DictConfigSerializer(schema=datasourceConfigSchema),
        )
        # As the store is in-memory, it needs to be populated immediately
        datasources = cast(Dict[str, DatasourceConfig], self.config.datasources or {})
        for name, config in datasources.items():
            datasource_store.add_by_name(datasource_name=name, datasource_config=config)

        return datasource_store

    @public_api
    def convert_to_file_context(self) -> FileDataContext:
        """Convert existing EphemeralDataContext into a FileDataContext.

        Scaffolds a file-backed project structure in the current working directory.

        Returns:
            A FileDataContext with an updated config to reflect the state of the current context.
        """
        self._synchronize_fluent_datasources()
        migrator = FileMigrator(
            primary_stores=self.stores,
            datasource_store=self._datasource_store,
            variables=self.variables,
            fluent_config=self.fluent_config,
        )
        return migrator.migrate()
