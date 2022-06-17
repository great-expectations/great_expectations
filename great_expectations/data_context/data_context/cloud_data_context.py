import configparser
import logging
import os
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
        ge_cloud_mode: bool = True,
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

    @classmethod
    def _get_global_config_value(
        cls,
        environment_variable: str,
        conf_file_section: Optional[str] = None,
        conf_file_option: Optional[str] = None,
    ) -> Optional[str]:
        """
        Overrides _get_global_config_value() method in AbstractDataContext to allow for conf_file_section
        and conf_file_option. Currently this is called by DataContext
        Args:
            environment_variable (str):
            conf_file_section (str):
            conf_file_option (str):

        Returns:
            Optional string corresponding to config_value
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
