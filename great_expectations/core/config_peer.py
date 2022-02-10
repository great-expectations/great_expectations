import logging
from abc import ABC, abstractmethod
from typing import Union

from great_expectations.data_context.types.base import BaseYamlConfig
from great_expectations.util import filter_properties_dict

logger = logging.getLogger(__name__)


class ConfigPeer(ABC):
    """
    A ConfigPeer is an object, whose subclasses can be instantiated using the "instantiate_class_from_config()" method
    (located in "great_expectations/util.py").  Its immediate descendant subclass must use a subclass of
    "BaseYamlConfig" as an argument to its constructor, and the subsequent descentants must use only primitive types as
    their constructor arguments, whereever keys correspond to the keys of the "BaseYamlConfig" immutable configuration
    object counterpart. The name, "ConfigPeer", denotes the fact that every immediate descendant subclass must have an
    immutable, Marshmallow Schema validated, configuration class as its peer.
    """

    @property
    @abstractmethod
    def config(self) -> BaseYamlConfig:
        pass

    def get_config(
        self,
        mode: str = "typed",
        **kwargs,
    ) -> Union[BaseYamlConfig, dict, str]:
        config: BaseYamlConfig = self.config

        if mode == "typed":
            return config

        if mode == "commented_map":
            return config.commented_map

        if mode == "yaml":
            return config.to_yaml_str()

        if mode == "dict":
            config_kwargs: dict = config.to_dict()
        elif mode == "json_dict":
            config_kwargs: dict = config.to_json_dict()
        else:
            raise ValueError(f'Unknown mode {mode} in "BaseCheckpoint.get_config()".')

        kwargs["inplace"] = True
        filter_properties_dict(
            properties=config_kwargs,
            **kwargs,
        )

        return config_kwargs

    def __repr__(self) -> str:
        return str(self.get_config())
