import logging
from typing import Mapping, Union

from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context_variables import (
    CloudDataContextVariables,
)
from great_expectations.data_context.types.base import DataContextConfig, GeCloudConfig

logger = logging.getLogger(__name__)


class CloudDataContext(AbstractDataContext):
    """
    Subclass of AbstractDataContext that contains functionality necessary to hydrate state from cloud
    """

    def __init__(
        self,
        project_config: Union[DataContextConfig, Mapping],
        runtime_environment: dict,
        ge_cloud_config: GeCloudConfig,
    ) -> None:
        """
        CloudDataContext constructor

        Args:
            project_config (DataContextConfig): config for CloudDataContext
            runtime_environment (dict):  a dictionary of config variables that override both those set in
                config_variables.yml and the environment
            ge_cloud_config (GeCloudConfig): GeCloudConfig corresponding to current CloudDataContext
        """
        super().__init__(runtime_environment=runtime_environment)
        self._ge_cloud_mode = True  # property needed for backward compatibility
        self._ge_cloud_config = ge_cloud_config
        self._project_config = self._apply_global_config_overrides(
            config=project_config
        )

    def _init_variables(self) -> CloudDataContextVariables:
        raise NotImplementedError
