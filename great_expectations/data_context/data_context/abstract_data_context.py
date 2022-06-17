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
        Applies global confirmation overrides for
            - usage_statistics being enabled
            - data_context_id
            - global_usage_statistics_url

        Args:
            config (DataContextConfig): Config that is passed into the DataContext constructor

        Returns:
            DataContextConfig with the appropriate overrides

        """
        validation_errors: dict = {}
        config_with_global_config_overrides: DataContextConfig = copy.deepcopy(config)
        usage_stats_enabled: bool = self._get_global_usage_statistics_override()
        if not usage_stats_enabled:  # then usage_statistics is false
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
        # usage_statistics_url
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

    @classmethod
    def _get_global_config_value(cls, environment_variable: str) -> Optional[str]:
        """
        Method to retrieve config value.

        This method can be overridden in child classes (like FileDataContext) when we need to look for
        config values in other locations like config files.
        Args:
            environment_variable (str): name of environment_variable to retrieve

        Returns:
            value of global_config_value

        """
        # Overridden when necessary in child classes
        return cls._get_config_value_from_env_var(
            environment_variable=environment_variable
        )

    @classmethod
    def _get_config_value_from_env_var(
        cls,
        environment_variable: Optional[str] = None,
    ) -> Optional[str]:
        """
        Method fo retrieve config value from environment variable.

        Args:
            environment_variable (str): name of environment variable to retrieve

        Returns:

            value of environment_variable which can also be None

        """
        if environment_variable and os.environ.get(environment_variable, False):
            return os.environ.get(environment_variable)
        else:
            return None

    def _get_global_usage_statistics_override(self) -> bool:
        """
        Method to retrieve config value.

        This method can be overridden in child classes (like FileDataContext) when we need to look for
        config values in other locations like config files.

        Returns:
            bool that tells you whether usage_statistics is on or off

        """
        return self._check_global_usage_statistics_env_var()

    @staticmethod
    def _check_global_usage_statistics_env_var() -> bool:
        """
        Checks environment variable to see if GE_USAGE_STATS exists as an environment variable

        If GE_USAGE_STATS exists AND its value is one of the FALSEY_STRINGS, usage_statistics is disabled (return False)
        Return True otherwise.

        This method can be overridden in child classes (like FileDataContext) when we need to look for
        config values in other locations like config files.

        Returns:
            bool that tells you whether usage_statistics is on or off

        """
        if os.environ.get("GE_USAGE_STATS", False):
            ge_usage_stats = os.environ.get("GE_USAGE_STATS")
            if ge_usage_stats in AbstractDataContext.FALSEY_STRINGS:
                return False
            else:
                logger.warning(
                    "GE_USAGE_STATS environment variable must be one of: {}".format(
                        AbstractDataContext.FALSEY_STRINGS
                    )
                )
        return True

    def _get_data_context_id_override(self) -> Optional[str]:
        """
        Return data_context_id from environment variable.

        Returns:
            Optional string that represents data_context_id
        """
        return self._get_data_context_id_override_from_env_var()

    def _get_data_context_id_override_from_env_var(self) -> Optional[str]:
        """
        Checks environment variable to see if GE_DATA_CONTEXT_ID exists as an environment variable

        Returns:
            Optional string that represents data_context_id
        """
        return self._get_global_config_value(environment_variable="GE_DATA_CONTEXT_ID")

    def _get_usage_stats_url_override(self) -> Optional[str]:
        """
        Return GE_USAGE_STATISTICS_URL from environment variable if it exists

        Returns:
            Optional string that represents GE_USAGE_STATISTICS_URL
        """
        return self._get_config_value_from_env_var()

    def _get_usage_stats_url_override_from_env_var(self) -> Optional[str]:
        """
        Checks environment variable to see if GE_USAGE_STATISTICS_URL exists as an environment variable

        Returns:
            Optional string that represents GE_USAGE_STATISTICS_URL

        """
        return self._get_global_config_value(
            environment_variable="GE_USAGE_STATISTICS_URL"
        )
