from six import string_types

from ..types import (
    DotDict,
    RequiredKeysDotDict,
    AllowedKeysDotDict,
    ListOf,
)

class ActionInternalConfig(RequiredKeysDotDict):
    """A typed object containing the kwargs for a specific subclass of ValidationAction.
    """
    pass

class ActionConfig(AllowedKeysDotDict):
    _allowed_keys = set([
        "module_name",
        "class_name",
        "kwargs"
    ])
    _key_types = {
        "module_name" : string_types,
        "class_name" : string_types,
        "kwargs" : ActionInternalConfig,
    }

class ActionSetConfig(AllowedKeysDotDict):
    _allowed_keys = set([
        "module_name",
        "class_name",
        "action_list",
    ])

    _key_types = {
        "module_name" : string_types,
        "class_name" : string_types,
        "action_list" : ListOf(ActionConfig),
    }