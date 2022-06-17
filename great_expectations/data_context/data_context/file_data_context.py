import configparser
import logging
import os
from typing import Mapping, Optional, Union

from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context.types.data_context_variables import (
    FileDataContextVariables,
)

logger = logging.getLogger(__name__)


class FileDataContext(AbstractDataContext):
    """
    Extends AbstractDataContext, contains only functionality necessary to hydrate state from disk.

    TODO: Most of the functionality in DataContext will be refactored into this class, and the current DataContext
    class will exist only for backwards-compatibility reasons.
    """

    def __init__(
        self,
        project_config: Union[DataContextConfig, Mapping],
        context_root_dir: Optional[str] = None,
        runtime_environment: Optional[dict] = None,
    ) -> None:
        """FileDataContext constructor

        Args:
            project_config (DataContextConfig):  Config for current DataContext
            context_root_dir (Optional[str]): location to look for the ``great_expectations.yml`` file. If None,
                searches for the file based on conventions for project subdirectories.
            runtime_environment (Optional[dict]): a dictionary of config variables that override both those set in
                config_variables.yml and the environment
        """
        self.runtime_environment = runtime_environment or {}
        self._context_root_dir = context_root_dir
        self._project_config = self._apply_global_config_overrides(
            config=project_config
        )

    def _init_variables(self) -> FileDataContextVariables:
        raise NotImplementedError

    def _check_global_usage_statistics_opt_out(self) -> bool:
        """
        Method to retrieve usage_statistics_enabled config value.
            calls __check_global_usage_statistics_env_var_and_file_opt_out, which will look for override in
            environment variable and config file
        Returns:
            bool that tells you whether usage_statistics is on or off
        """
        return self._check_global_usage_statistics_env_var_and_file_opt_out()

    @staticmethod
    def _check_global_usage_statistics_env_var_and_file_opt_out() -> bool:
        """
        Checks environment variable to see if GE_USAGE_STATS exists as an environment variable
        If GE_USAGE_STATS exists AND its value is one of the FALSEY_STRINGS, usage_statistics is disabled (return True)
        Return False otherwise.

        Also checks GLOBAL_CONFIG_PATHS to see if config file contains override for anonymous_usage_statistics

        Returns:
            bool that tells you whether usage_statistics is on or off
        """
        if os.environ.get("GE_USAGE_STATS", False):
            ge_usage_stats = os.environ.get("GE_USAGE_STATS")
            if ge_usage_stats in AbstractDataContext.FALSEY_STRINGS:
                return True
            else:
                logger.warning(
                    "GE_USAGE_STATS environment variable must be one of: {}".format(
                        AbstractDataContext.FALSEY_STRINGS
                    )
                )
        for config_path in AbstractDataContext.GLOBAL_CONFIG_PATHS:
            config = configparser.ConfigParser()
            states = config.BOOLEAN_STATES
            for falsey_string in AbstractDataContext.FALSEY_STRINGS:
                states[falsey_string] = False
            states["TRUE"] = True
            states["True"] = True
            config.BOOLEAN_STATES = states
            config.read(config_path)
            try:
                if config.getboolean("anonymous_usage_statistics", "enabled") is False:
                    # If stats are disabled, then opt out is true
                    return True
            except (ValueError, configparser.Error):
                pass
        return False

    @classmethod
    def _get_global_config_value(
        cls,
        environment_variable: str,
        conf_file_section: Optional[str] = None,
        conf_file_option: Optional[str] = None,
    ) -> Optional[str]:
        """
        Method to retrieve config value.
        Looks for config value in environment_variable and config file section

        Args:
            environment_variable (str): name of environment_variable to retrieve
            conf_file_section (str): section of config
            conf_file_option (str): key in section

        Returns:
            Optional string representing config value
        """
        assert (conf_file_section and conf_file_option) or (
            not conf_file_section and not conf_file_option
        ), "Must pass both 'conf_file_section' and 'conf_file_option' or neither."
        if environment_variable and os.environ.get(environment_variable, False):
            return os.environ.get(environment_variable)
        if conf_file_section and conf_file_option:
            for config_path in AbstractDataContext.GLOBAL_CONFIG_PATHS:
                config = configparser.ConfigParser()
                config.read(config_path)
                config_value = config.get(
                    conf_file_section, conf_file_option, fallback=None
                )
                if config_value:
                    return config_value
        return None

    def _get_data_context_id_override(self) -> Optional[str]:
        """
        Checks environment variable and conf to see if GE_DATA_CONTEXT_ID exists as an environment variable

        Returns:
            Optional string that represents data_context_id for usage_statistics
        """
        return self._get_data_context_id_override_from_env_var_and_file()

    def _get_data_context_id_override_from_env_var_and_file(self) -> Optional[str]:
        """
        Checks environment variable to see if GE_DATA_CONTEXT_ID exists as an environment variable
            or section in conf file


        Returns:
            Optional string that represents data_context_id
        """
        return self._get_global_config_value(
            environment_variable="GE_DATA_CONTEXT_ID",
            conf_file_section="anonymous_usage_statistics",
            conf_file_option="data_context_id",
        )

    def _get_usage_stats_url_override(self) -> Optional[str]:
        """
        Return GE_USAGE_STATISTICS_URL if it exists in env variable or conf file

        Returns:
            Optional string that represents GE_USAGE_STATISTICS_URL
        """
        return self._get_config_value_from_env_var_and_file()

    def _get_config_value_from_env_var_and_file(self) -> Optional[str]:
        """
        Checks environment variable + config file to see if GE_USAGE_STATISTICS_URL exists

        Returns:
           Optional string that represents GE_USAGE_STATISTICS_URL
        """
        return self._get_global_config_value(
            environment_variable="GE_USAGE_STATISTICS_URL",
            conf_file_section="anonymous_usage_statistics",
            conf_file_option="usage_statistics_url",
        )
