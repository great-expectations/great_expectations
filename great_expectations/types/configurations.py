from six import string_types
from ruamel.yaml import YAML, yaml_object


from great_expectations.types import AllowedKeysDotDict

yaml = YAML()


class Config(AllowedKeysDotDict):
    pass


@yaml_object(yaml)
class ClassConfig(Config):
    """Defines information sufficient to identify a class to be (dynamically) loaded for a DataContext."""
    _allowed_keys = {
        "module_name",
        "class_name"
    }
    _required_keys = {
        "class_name"
    }
    _key_types = {
        "module_name": string_types,
        "class_name": string_types
    }
