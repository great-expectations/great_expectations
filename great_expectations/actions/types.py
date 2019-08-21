from six import string_types

from ..types import (
    DotDict,
    LooselyTypedDotDict,
    ListOf,
)

class ActionInternalConfig(DotDict):
    pass

class ActionConfig(LooselyTypedDotDict):
    _allowed_keys = set([
        "module_name",
        "class_name",
        "kwargs"
    ])
    _key_types = {
        "module_name" : str, #This should be string_types. Need to merge in fixes to LooselyTypedDataDcit before that will work, though...
        "class_name" : str, #This should be string_types. Need to merge in fixes to LooselyTypedDataDcit before that will work, though...
        "kwargs" : ActionInternalConfig,
    }

class ActionSetConfig(LooselyTypedDotDict):
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