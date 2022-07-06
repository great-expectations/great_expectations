import logging
from typing import Mapping, Optional, Union

from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context_variables import (
    EphemeralDataContextVariables,
)
from great_expectations.data_context.types.base import DataContextConfig

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
        project_config: Union[DataContextConfig, Mapping],
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
        self._config_variables = self._load_config_variables()
        super().__init__(runtime_environment=runtime_environment)

    def _init_variables(self) -> EphemeralDataContextVariables:
        raise NotImplementedError
