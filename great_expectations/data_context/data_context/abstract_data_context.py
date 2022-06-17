import configparser
import copy
import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Mapping, Optional, Union

from great_expectations.data_context.types.base import (
    DataContextConfig,
    anonymizedUsageStatisticsSchema,
)

logger = logging.getLogger(__name__)


class AbstractDataContext(ABC):
    """
    Base class for all DataContexts that contain all context-agnostic data context operations.

    The class encapsulates most store / core components and convenience methods used to access them, meaning the
    majority of DataContext functionality lives here.
    """

    FALSEY_STRINGS = ["FALSE", "false", "False", "f", "F", "0"]
    GLOBAL_CONFIG_PATHS = [
        os.path.expanduser("~/.great_expectations/great_expectations.conf"),
        "/etc/great_expectations.conf",
    ]

    @abstractmethod
    def _init_variables(self) -> None:
        raise NotImplementedError

    def _apply_global_config_overrides(
        self, config: Union[DataContextConfig, Mapping]
    ) -> DataContextConfig:

        """
        Applies global configuration overrides for
            - usage_statistics being enabled
            - data_context_id for usage_statistics
            - global_usage_statistics_url

        Args:
            config (DataContextConfig): Config that is passed into the DataContext constructor

        Returns:
            DataContextConfig with the appropriate overrides
        """
        validation_errors: dict = {}
        config_with_global_config_overrides: DataContextConfig = copy.deepcopy(config)
        usage_stats_opted_out: bool = self._check_global_usage_statistics_opt_out()
        # if usage_stats_opted_out then usage_statistics is false
        # TODO: Refactor so that this becomes usage_stats_enabled (and we don't have to flip the boolean in our minds)
        if usage_stats_opted_out:
            logger.info(
                "Usage statistics is disabled globally. Applying override to project_config."
            )
            config_with_global_config_overrides.anonymous_usage_statistics.enabled = (
                False
            )
        global_data_context_id: Optional[str] = self._get_data_context_id_override()
        # data_context_id
        if global_data_context_id:
            data_context_id_errors = anonymizedUsageStatisticsSchema.validate(
                {"data_context_id": global_data_context_id}
            )
            if not data_context_id_errors:
                logger.info(
                    "data_context_id is defined globally. Applying override to project_config."
                )
                config_with_global_config_overrides.anonymous_usage_statistics.data_context_id = (
                    global_data_context_id
                )
            else:
                validation_errors.update(data_context_id_errors)

        # usage statistics url
        global_usage_statistics_url: Optional[
            str
        ] = self._get_usage_stats_url_override()
        if global_usage_statistics_url:
            usage_statistics_url_errors = anonymizedUsageStatisticsSchema.validate(
                {"usage_statistics_url": global_usage_statistics_url}
            )
            if not usage_statistics_url_errors:
                logger.info(
                    "usage_statistics_url is defined globally. Applying override to project_config."
                )
                config_with_global_config_overrides.anonymous_usage_statistics.usage_statistics_url = (
                    global_usage_statistics_url
                )
            else:
                validation_errors.update(usage_statistics_url_errors)
        if validation_errors:
            logger.warning(
                "The following globally-defined config variables failed validation:\n{}\n\n"
                "Please fix the variables if you would like to apply global values to project_config.".format(
                    json.dumps(validation_errors, indent=2)
                )
            )

        return config_with_global_config_overrides

    @staticmethod
    def _get_global_config_value(
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

    def _check_global_usage_statistics_opt_out(self) -> bool:
        """
        Method to retrieve config value.
        This method can be overridden in child classes (like FileDataContext) when we need to look for
        config values in other locations like config files.

        Returns:
            bool that tells you whether usage_statistics is opted out
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

    def _get_data_context_id_override(self) -> Optional[str]:
        """
        Return data_context_id from environment variable.

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
        Return GE_USAGE_STATISTICS_URL from environment variable if it exists

        Returns:
            Optional string that represents GE_USAGE_STATISTICS_URL
        """
        return self._get_global_config_value(
            environment_variable="GE_USAGE_STATISTICS_URL",
            conf_file_section="anonymous_usage_statistics",
            conf_file_option="usage_statistics_url",
        )
