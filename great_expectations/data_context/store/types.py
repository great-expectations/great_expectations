from ...types import LooselyTypedDotDict

# #This is a bag of 
# class StoreConfigKwargs(LooselyTypedDotDict):
#     _allowed_keys = set([
#         "serialization_type"
#     ])

class StoreMetaConfig(LooselyTypedDotDict):
    _allowed_keys = set([
        "module_name",
        "class_name",
        "store_config",
    ])
    _required_keys = set([
        "module_name",
        "class_name",
    ])
    _key_types = {
        "module_name": str,
        "class_name": str,
        # "store_class_config": dict,
    }

class InMemoryStoreConfig(LooselyTypedDotDict):
    _allowed_keys = set([
        "serialization_type"
    ])


class FilesystemStoreConfig(LooselyTypedDotDict):
    _allowed_keys = set([
        "base_directory"
    ])
