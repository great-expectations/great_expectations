from six import string_types

from ..types import (
    DotDict,
    RequiredKeysDotDict,
    AllowedKeysDotDict,
    ListOf,
)

# NOTE : Some day it might make sense to implement 
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
        "module_name" : str, #TODO: This should be string_types. Need to merge in fixes to LooselyTypedDataDcit before that will work, though...
        "class_name" : str, #TODO: This should be string_types. Need to merge in fixes to LooselyTypedDataDcit before that will work, though...
        #TODO: Add this back in.
        # "kwargs" : ActionInternalConfig,
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
        #TODO: This should be a DictOf, not a ListOf.
        "action_list" : ListOf(ActionConfig),
    }