import logging
from typing import Optional

from great_expectations.core.serializer import DictConfigSerializer
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context_variables import (
    EphemeralDataContextVariables,
)
from great_expectations.data_context.types.base import (
    DataContextConfig,
    datasourceConfigSchema,
)

logger = logging.getLogger(__name__)


class EphemeralDataContext(AbstractDataContext):
    """
    Will contain functionality to create DataContext at runtime (ie. passed in config object or from stores). Users will
    be able to use EphemeralDataContext for having a temporary or in-memory DataContext

    TODO: Most of the BaseDataContext code will be migrated to this class, which will continue to exist for backwards
    compatibility reasons.
    """

    def __init__(
        self,
        project_config: DataContextConfig,
        runtime_environment: Optional[dict] = None,
    ) -> None:
        """EphemeralDataContext constructor

        project_config: config for in-memory EphemeralDataContext
        runtime_environment: a dictionary of config variables tha
                override both those set in config_variables.yml and the environment

        """
        self._project_config = self._apply_global_config_overrides(
            config=project_config
        )
        super().__init__(runtime_environment=runtime_environment)

    def _init_variables(self) -> EphemeralDataContextVariables:
        variables = EphemeralDataContextVariables(
            config=self._project_config,
            config_provider=self.config_provider,
        )
        return variables

    def _init_datasource_store(self) -> None:
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
        self._datasource_store = datasource_store
