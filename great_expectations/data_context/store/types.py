from six import string_types

from ...types import AllowedKeysDotDict

class StoreMetaConfig(AllowedKeysDotDict):
    """Top-level configs for stores look like this
    """
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


class InMemoryStoreConfig(AllowedKeysDotDict):
    _allowed_keys = set([
        "serialization_type"
    ])

class NamespacedInMemoryStoreConfig(AllowedKeysDotDict):
    _allowed_keys = set([
        "serialization_type",
        "resource_identifier_class_name",
    ])
    _required_keys = set([
        "resource_identifier_class_name"
    ])


class FilesystemStoreConfig(AllowedKeysDotDict):
    _allowed_keys = set([
        "serialization_type",
        "base_directory",
        "file_prefix",
        "file_extension",
        "compression",
    ])

    _required_keys = set([
        "base_directory",
        "file_extension",
    ])


class NamespacedFilesystemStoreConfig(AllowedKeysDotDict):
    _allowed_keys = set([
        "resource_identifier_class_name",
        "serialization_type",
        "base_directory",
        "file_prefix",
        "file_extension",
        "compression",
    ])

    _required_keys = set([
        "resource_identifier_class_name",
        "base_directory",
        "file_extension",
    ])

    _key_types = {
        "resource_identifier_class_name" : string_types,
    }
