"""
TODO
"""
from __future__ import annotations

import errno
import os
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import Dict, Optional, Type, cast

from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.types.base import GeCloudConfig
from great_expectations.data_context.util import (
    substitute_all_config_variables,
    substitute_config_variable,
)

yaml = YAMLHandler()


class AbstractConfigurationProvider(ABC):
    @abstractmethod
    def get_values(self) -> Dict[str, str]:
        """
        TODO
        """
        pass


class ConfigurationProvider(AbstractConfigurationProvider):
    """
    TODO
    """

    def __init__(self) -> None:
        self._providers: OrderedDict[
            Type[AbstractConfigurationProvider], AbstractConfigurationProvider
        ] = OrderedDict()

    def register_provider(self, provider: AbstractConfigurationProvider):
        """
        TODO
        """
        type_ = type(provider)
        if type_ in self._providers:
            raise ValueError(f"Provider of type {type_} has already been registered!")
        self._providers[type_] = provider

    def get_provider(
        self, type_: Type[AbstractConfigurationProvider]
    ) -> Optional[AbstractConfigurationProvider]:
        """
        TODO
        """
        return self._providers.get(type_)

    def get_values(self) -> Dict[str, str]:
        """
        TODO
        """
        values: Dict[str, str] = {}
        for provider in self._providers.values():
            # In the case a provider's values use ${VARIABLE} syntax, look at existing values
            # and perform substitutions before adding to the result obj.
            provider_values = provider.get_values()
            if values:
                provider_values = cast(
                    dict, substitute_all_config_variables(provider_values, values)
                )
            values.update(provider_values)
        return values


class RuntimeEnvironmentConfigurationProvider(AbstractConfigurationProvider):
    """
    TODO
    """

    def __init__(self, runtime_environment: Dict[str, str]) -> None:
        self._runtime_environment = runtime_environment

    def get_values(self) -> Dict[str, str]:
        return self._runtime_environment


class EnvironmentConfigurationProvider(AbstractConfigurationProvider):
    """
    TODO
    """

    def get_values(self) -> Dict[str, str]:
        return dict(os.environ)


class ConfigurationVariablesConfigurationProvider(AbstractConfigurationProvider):
    """
    TODO
    """

    def __init__(
        self, config_variables_file_path: str, root_directory: Optional[str] = None
    ) -> None:
        self._config_variables_file_path = config_variables_file_path
        self._root_directory = root_directory

    def get_values(self) -> Dict[str, str]:
        try:
            # If the user specifies the config variable path with an environment variable, we want to substitute it
            defined_path: str = substitute_config_variable(  # type: ignore[assignment]
                self._config_variables_file_path, dict(os.environ)
            )
            if not os.path.isabs(defined_path):
                # A BaseDataContext will not have a root directory; in that case use the current directory
                # for any non-absolute path
                root_directory: str = self._root_directory or os.curdir
            else:
                root_directory = ""
            var_path = os.path.join(root_directory, defined_path)
            with open(var_path) as config_variables_file:
                contents = config_variables_file.read()

            variables = yaml.load(contents) or {}
            return dict(variables)

        except OSError as e:
            if e.errno != errno.ENOENT:
                raise
            return {}


class CloudConfigurationProvider(AbstractConfigurationProvider):
    """
    TODO
    """

    def __init__(self, cloud_config: GeCloudConfig) -> None:
        self._cloud_config = cloud_config

    def get_values(self) -> Dict[str, str]:
        from great_expectations.data_context.data_context.cloud_data_context import (
            GECloudEnvironmentVariable,
        )

        return cast(
            Dict[str, str],
            {
                GECloudEnvironmentVariable.BASE_URL: self._cloud_config.base_url,
                GECloudEnvironmentVariable.ACCESS_TOKEN: self._cloud_config.access_token,
                GECloudEnvironmentVariable.ORGANIZATION_ID: self._cloud_config.organization_id,
            },
        )
