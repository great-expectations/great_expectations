import logging
from typing import Mapping, Optional, Union

from great_expectations.data_context import FileDataContext
from great_expectations.data_context.types.base import (
    DEFAULT_USAGE_STATISTICS_URL,
    DataContextConfig,
    DataContextConfigDefaults,
    GeCloudConfig,
)
from great_expectations.data_context.types.data_context_variables import (
    CloudDataContextVariables,
)
from great_expectations.data_context.util import substitute_all_config_variables

logger = logging.getLogger(__name__)


class CloudDataContext(FileDataContext):
    """
    Subclass of FileDataContext that contains functionality necessary to hydrate state from cloud
    """

    def __init__(
        self,
        project_config: Union[DataContextConfig, Mapping],
        context_root_dir: str,
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
        self._ge_cloud_mode = True  # property needed for backward compatibility
        self._ge_cloud_config = ge_cloud_config
        self._context_root_directory = context_root_dir
        self._project_config = self._apply_global_config_overrides(
            config=project_config
        )
        super().__init__(runtime_environment=runtime_environment)

    def _init_variables(self) -> CloudDataContextVariables:
        raise NotImplementedError

    def get_config_with_variables_substituted(
        self, config: Optional[DataContextConfig] = None
    ) -> DataContextConfig:
        """
        Substitute vars in config of form ${var} or $(var) with values found in the following places,
        in order of precedence: ge_cloud_config (for Data Contexts in GE Cloud mode), runtime_environment,
        environment variables, config_variables, or ge_cloud_config_variable_defaults (allows certain variables to
        be optional in GE Cloud mode).
        """
        if not config:
            config = self.config

        substitutions: dict = self._determine_substitutions()

        ge_cloud_config_variable_defaults = {
            "plugins_directory": self._normalize_absolute_or_relative_path(
                DataContextConfigDefaults.DEFAULT_PLUGINS_DIRECTORY.value
            ),
            "usage_statistics_url": DEFAULT_USAGE_STATISTICS_URL,
        }
        for config_variable, value in ge_cloud_config_variable_defaults.items():
            if substitutions.get(config_variable) is None:
                logger.info(
                    f'Config variable "{config_variable}" was not found in environment or global config ('
                    f'{self.GLOBAL_CONFIG_PATHS}). Using default value "{value}" instead. If you would '
                    f"like to "
                    f"use a different value, please specify it in an environment variable or in a "
                    f"great_expectations.conf file located at one of the above paths, in a section named "
                    f'"ge_cloud_config".'
                )
                substitutions[config_variable] = value

        return DataContextConfig(
            **substitute_all_config_variables(
                config, substitutions, self.DOLLAR_SIGN_ESCAPE_STRING
            )
        )
