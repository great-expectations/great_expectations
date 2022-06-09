import logging
import os
from typing import Mapping, Optional, Union

from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.types.base import (
    DEFAULT_USAGE_STATISTICS_URL,
    DataContextConfig,
    DataContextConfigDefaults,
    GeCloudConfig,
)
from great_expectations.data_context.util import substitute_all_config_variables

logger = logging.getLogger(__name__)


class CloudDataContext(AbstractDataContext):
    """
    CloudDataContext is actually a subclass of FileDataContext in its currents tate
    """

    def __init__(
        self,
        project_config: Union[DataContextConfig, Mapping],
        runtime_environment: Optional[dict] = None,
        ge_cloud_mode: bool = False,
        ge_cloud_config: Optional[GeCloudConfig] = None,
    ) -> None:

        self._ge_cloud_mode = ge_cloud_mode
        self._ge_cloud_config = ge_cloud_config
        self.runtime_environment = runtime_environment or {}

        # config overrides with cloud configs
        self._project_config = project_config
        # TODO: this is actually unnecessary technically speaking. see if it can actually be removed
        # super()._apply_global_config_overrides()
        # We want to have directories set up before initializing usage statistics so that we can obtain a context instance id
        self._in_memory_instance_id = (
            None  # This variable *may* be used in case we cannot save an instance id
        )
        # Init data_context_id
        self._data_context_id = self._construct_data_context_id()

        super().__init__(
            project_config=project_config, runtime_environment=runtime_environment
        )

    @property
    def config(self) -> DataContextConfig:
        return self._project_config

    @property
    def ge_cloud_config(self) -> Optional[GeCloudConfig]:
        return self._ge_cloud_config

    @property
    def ge_cloud_mode(self) -> bool:
        return self._ge_cloud_mode

    @staticmethod
    def _normalize_absolute_or_relative_path(path: Optional[str]) -> Optional[str]:
        """
        The Cloud-DataContext version of this method. Different from FileDataContext in th
        Args:
            path ():

        Returns:

        """
        if path is None:
            return
        if os.path.isabs(path):
            return path
        else:
            return

    def get_config_with_variables_substituted(self, config=None) -> DataContextConfig:
        """
        Substitute vars in config of form ${var} or $(var) with values found in the following places,
        in order of precedence: ge_cloud_config (for Data Contexts in GE Cloud mode), runtime_environment,
        environment variables, config_variables, or ge_cloud_config_variable_defaults (allows certain variables to
        be optional in GE Cloud mode).
        """
        if not config:
            config = self.config
        # config_variables is a file specific thing
        substituted_config_variables = substitute_all_config_variables(
            config,
            dict(os.environ),
            self.DOLLAR_SIGN_ESCAPE_STRING,
        )

        # Substitutions should have already occurred for GE Cloud configs at this point
        substitutions = {
            **substituted_config_variables,
            **dict(os.environ),
            **self.runtime_environment,
        }

        # is the cloud all going to be local?
        ge_cloud_config_variable_defaults: dict = {
            "plugins_directory": self._normalize_absolute_or_relative_path(
                DataContextConfigDefaults.DEFAULT_PLUGINS_DIRECTORY.value
            ),
            "usage_statistics_url": DEFAULT_USAGE_STATISTICS_URL,
        }
        for config_variable, value in ge_cloud_config_variable_defaults.items():
            if substitutions.get(config_variable) is None:
                logger.info(
                    f'Config variable "{config_variable}" was not found in environment or global config ('
                    f'{AbstractDataContext.GLOBAL_CONFIG_PATHS}). Using default value "{value}" instead. If you would '
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

    def _construct_data_context_id(self) -> str:
        """
        Choose the id in the currently-configured ge_cloud_config
        Returns:
            UUID to use as the data_context_id
        """
        return self.ge_cloud_config.organization_id
