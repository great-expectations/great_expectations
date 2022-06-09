from typing import Mapping, Optional, Union

from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.types.base import DataContextConfig


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
    ):
        self.runtime_environment = runtime_environment or {}
        super().__init__(
            project_config=project_config, runtime_environment=runtime_environment
        )

    # TODO: determine if we actually need this
    # def get_config_with_variables_substituted(self, config=None) -> DataContextConfig:
    #     """
    #     Substitute vars in config of form ${var} or $(var) with values found in the following places,
    #     in order of precedence: ge_cloud_config (for Data Contexts in GE Cloud mode), runtime_environment,
    #     environment variables, config_variables, or ge_cloud_config_variable_defaults (allows certain variables to
    #     be optional in GE Cloud mode).
    #     """
    #     if not config:
    #         config = self.config
    #
    #     substituted_config_variables = substitute_all_config_variables(
    #         self.config_variables,
    #         dict(os.environ),
    #         self.DOLLAR_SIGN_ESCAPE_STRING,
    #     )
    #
    #     # Substitutions should have already occurred for GE Cloud configs at this point
    #     substitutions = {
    #         **substituted_config_variables,
    #         **dict(os.environ),
    #         **self.runtime_environment,
    #     }
    #
    #     if self.ge_cloud_mode:
    #         ge_cloud_config_variable_defaults = {
    #             "plugins_directory": self._normalize_absolute_or_relative_path(
    #                 DataContextConfigDefaults.DEFAULT_PLUGINS_DIRECTORY.value
    #             ),
    #             "usage_statistics_url": DEFAULT_USAGE_STATISTICS_URL,
    #         }
    #         for config_variable, value in ge_cloud_config_variable_defaults.items():
    #             if substitutions.get(config_variable) is None:
    #                 logger.info(
    #                     f'Config variable "{config_variable}" was not found in environment or global config ('
    #                     f'{self.GLOBAL_CONFIG_PATHS}). Using default value "{value}" instead. If you would '
    #                     f"like to "
    #                     f"use a different value, please specify it in an environment variable or in a "
    #                     f"great_expectations.conf file located at one of the above paths, in a section named "
    #                     f'"ge_cloud_config".'
    #                 )
    #                 substitutions[config_variable] = value
    #
    #     return DataContextConfig(
    #         **substitute_all_config_variables(
    #             config, substitutions, self.DOLLAR_SIGN_ESCAPE_STRING
    #         )
    #     )
