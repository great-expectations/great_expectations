import logging
from typing import Mapping, Optional, Union

from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.types.base import DataContextConfig, GeCloudConfig
from great_expectations.data_context.types.data_context_variables import (
    CloudDataContextVariables,
)

logger = logging.getLogger(__name__)


class CloudDataContext(AbstractDataContext):
    """
    Subclass of AbstractDataContext that contains functionality necessary to hydrate state from cloud
    """

    def __init__(
        self,
        project_config: Union[DataContextConfig, Mapping],
        runtime_environment: Optional[dict] = None,
        ge_cloud_mode: bool = False,  # TODO: determine if this can be set to True for CloudDataContext
        ge_cloud_config: Optional[GeCloudConfig] = None,
    ) -> None:
        """
        CloudDataContext constructor

        Args:
            project_config (DataContextConfig): config for CloudDataContext
            runtime_environment (dict):  a dictionary of config variables that override both those set in
                config_variables.yml and the environment
            ge_cloud_mode (bool): is cloud_mode true? (default true)
            ge_cloud_config (GeCloudConfig): GeCloudConfig corresponding to current CloudDataContext
        """
        self._ge_cloud_mode = ge_cloud_mode
        self._ge_cloud_config = ge_cloud_config
        self.runtime_environment = runtime_environment or {}

        self._project_config = self._apply_global_config_overrides(
            config=project_config
        )

    def _init_variables(self) -> CloudDataContextVariables:
        raise NotImplementedError
