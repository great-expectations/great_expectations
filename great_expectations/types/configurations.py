from ruamel.yaml import YAML, yaml_object
from great_expectations.types import LooselyTypedDotDict
yaml = YAML()


@yaml_object(yaml)
class ClassConfig(LooselyTypedDotDict):
    """Defines information sufficient to identify a class to be (dynamically) loaded for a DataContext."""
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
