import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import Union

from great_expectations.data_context.types.base import BaseYamlConfig
from great_expectations.util import filter_properties_dict

logger = logging.getLogger(__name__)


class ConfigOutputModes(Enum):
    TYPED = "typed"
    COMMENTED_MAP = "commented_map"
    YAML = "yaml"
    DICT = "dict"
    JSON_DICT = "json_dict"


ConfigOutputModeType = Union[(ConfigOutputModes, str)]


class ConfigPeer(ABC):
    '\n    A ConfigPeer is an object, whose subclasses can be instantiated using instantiate_class_from_config() (located in\n    great_expectations/util.py).  Its immediate descendant subclass must use a subclass of BaseYamlConfig as an argument\n    to its constructor, and the subsequent descentants must use only primitive types as their constructor arguments,\n    whereever keys correspond to the keys of the "BaseYamlConfig" configuration object counterpart. The name ConfigPeer\n    means: Every immediate descendant subclass must have Marshmallow Schema validated configuration class as its peer.\n\n    # TODO: <Alex>2/11/2022</Alex>\n    When -- as part of a potential future architecture update -- serialization is decoupled from configuration, the\n    configuration objects, persitable as YAML files, will no longer inherit from the BaseYamlConfig class.  Rather,\n    any form of serialization (YAML, JSON, SQL Database Tables, Pickle, etc.) will apply as peers, independent of the\n    configuration classes themselves.  Hence, as part of this change, ConfigPeer will cease being the superclass of\n    business objects (such as BaseDataContext, BaseCheckpoint, and BaseRuleBasedProfiler).  Instead, every persitable\n    business object will contain a reference to its corresponding peer class, supporting the ConfigPeer interfaces.\n'

    @property
    @abstractmethod
    def config(self) -> BaseYamlConfig:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        pass

    def get_config(
        self, mode: ConfigOutputModeType = ConfigOutputModes.TYPED, **kwargs
    ) -> Union[(BaseYamlConfig, dict, str)]:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
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
            config_kwargs: dict = config.to_json_dict()
        else:
            raise ValueError(f'Unknown mode {mode} in "BaseCheckpoint.get_config()".')
        kwargs["inplace"] = True
        filter_properties_dict(properties=config_kwargs, **kwargs)
        return config_kwargs

    def __repr__(self) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return str(self.get_config())
