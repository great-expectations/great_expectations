from six import string_types

from ruamel.yaml import YAML, yaml_object
from great_expectations.types import LooselyTypedDotDict
yaml = YAML()


class Config(LooselyTypedDotDict):
    pass


@yaml_object(yaml)
class ClassConfig(Config):
    _allowed_keys = {
        "module_name",
        "class_name"
    }
    _required_keys = {
        "class_name"
    }
    _key_types = {
        "module_name": str,
        "class_name": str
    }


class DataContextConfig(Config):
    _allowed_keys = set([
        "plugins_directory",
        "datasources",
        "stores",
        "data_docs",  # TODO: Rename this to sites, to remove a layer of extraneous nesting
    ])

    _required_keys = set([
        "plugins_directory",
        "datasources",
        "stores",
        "data_docs",
    ])

    _key_types = {
        "plugins_directory": string_types,
        "datasources": dict,
        "stores": dict,
        "data_docs": dict,
    }
