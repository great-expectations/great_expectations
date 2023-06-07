from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING

from great_expectations.util import filter_properties_dict

if TYPE_CHECKING:
    from great_expectations.data_context.types.base import BaseYamlConfig

logger = logging.getLogger(__name__)


class ConfigOutputModes(str, Enum):
    TYPED = "typed"
    COMMENTED_MAP = "commented_map"
    YAML = "yaml"
    DICT = "dict"
    JSON_DICT = "json_dict"


class ConfigPeer(ABC):
    """
    A ConfigPeer is an object, whose subclasses can be instantiated using instantiate_class_from_config() (located in
    great_expectations/util.py).  Its immediate descendant subclass must use a subclass of BaseYamlConfig as an argument
    to its constructor, and the subsequent descendants must use only primitive types as their constructor arguments,
    wherever keys correspond to the keys of the "BaseYamlConfig" configuration object counterpart. The name ConfigPeer
    means: Every immediate descendant subclass must have Marshmallow Schema validated configuration class as its peer.

    # TODO: <Alex>2/11/2022</Alex>
    When -- as part of a potential future architecture update -- serialization is decoupled from configuration, the
    configuration objects, persistable as YAML files, will no longer inherit from the BaseYamlConfig class.  Rather,
    any form of serialization (YAML, JSON, SQL Database Tables, Pickle, etc.) will apply as peers, independent of the
    configuration classes themselves.  Hence, as part of this change, ConfigPeer will cease being the superclass of
    business objects (such as BaseDataContext, BaseCheckpoint, and BaseRuleBasedProfiler).  Instead, every persistable
    business object will contain a reference to its corresponding peer class, supporting the ConfigPeer interfaces.
    """

    @property
    @abstractmethod
    def config(self) -> BaseYamlConfig:
        pass

    def get_config(
        self,
        mode: ConfigOutputModes = ConfigOutputModes.TYPED,
        **kwargs,
    ) -> BaseYamlConfig | dict | str:
        if isinstance(mode, str):
            mode = ConfigOutputModes(mode.lower())

        config: BaseYamlConfig = self.config

        if mode == ConfigOutputModes.TYPED:
            return config

        if mode == ConfigOutputModes.COMMENTED_MAP:
            return config.commented_map

        if mode == ConfigOutputModes.YAML:
            return config.to_yaml_str()

        if mode == ConfigOutputModes.DICT:
            config_kwargs: dict = config.to_dict()
        elif mode == ConfigOutputModes.JSON_DICT:
            config_kwargs = config.to_json_dict()
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
