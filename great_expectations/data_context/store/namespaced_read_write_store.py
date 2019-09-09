import logging
logger = logging.getLogger(__name__)

import importlib
from six import string_types
import copy
import json

import pandas as pd

from ..types.base_resource_identifiers import (
    DataContextKey,
)
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from .store import (
    ReadWriteStore,
)
from great_expectations.data_context.util import (
    instantiate_class_from_config
)

class NamespacedReadWriteStore(ReadWriteStore):
    
    def __init__(self,
        store_backend,
        root_directory,
        serialization_type="json"
    ):
        self.store_backend = self._init_store_backend(
            copy.deepcopy(store_backend),
            runtime_config={
                "root_directory": root_directory
            }
        )
        self.root_directory = root_directory
        self.serialization_type = serialization_type

    def _init_store_backend(self, store_backend_config, runtime_config):
        raise NotImplementedError

    def _get(self, key):
        self._validate_key(key)

        key_tuple = self._convert_resource_identifier_to_tuple(key)
        return self.store_backend.get(key_tuple)

    def _set(self, key, serialized_value):
        self._validate_key(key)

        key_tuple = self._convert_resource_identifier_to_tuple(key)
        return self.store_backend.set(key_tuple, serialized_value)

    def list_keys(self):
        return [self._convert_tuple_to_resource_identifier(key) for key in self.store_backend.list_keys()]

    def has_key(self, key):
        # NOTE: This is not efficient
        return key in self.list_keys()

    def _convert_resource_identifier_to_tuple(self, key):
        # TODO : Optionally prepend a source_id (the frontend Store name) to the tuple.

        list_ = []
        list_ += self._convert_resource_identifier_to_list(key)

        return tuple(list_)

    def _convert_resource_identifier_to_list(self, key):
        # The logic in this function is recursive, so it can't return a tuple

        list_ = []
        #Fetch keys in _key_order to guarantee tuple ordering in both python 2 and 3
        for key_name in key._key_order:
            key_element = key[key_name]
            if isinstance( key_element, DataContextKey ):
                list_ += self._convert_resource_identifier_to_list(key_element)
            else:
                list_.append(key_element)

        return list_

    def _convert_tuple_to_resource_identifier(self, tuple_):
        new_identifier = self.key_class(*tuple_)
        return new_identifier

    def _validate_key(self, key):
        if not isinstance(key, self.key_class):
            raise TypeError("key: {!r} must be a {}, not {!r}".format(
                key,
                self.key_class,
                type(key),
            ))


class ExpectationStore(NamespacedReadWriteStore):
    # Note : As of 2019/09/06, this method is untested.
    # It shares virtually all of its business logic with ValidationStore, which is under test.

    def _init_store_backend(self, store_backend_config, runtime_config):
        self.key_class = ExpectationSuiteIdentifier

        if store_backend_config["class_name"] == "FixedLengthTupleFilesystemStoreBackend":
            config_defaults = {
                "key_length" : 4,
                "module_name" : "great_expectations.data_context.store",
                "filepath_template": '{0}/{1}/{2}/{3}.json',
            }
        else:
            config_defaults = {
                "module_name" : "great_expectations.data_context.store",
            }

        return instantiate_class_from_config(
            config=store_backend_config,
            runtime_config=runtime_config,
            config_defaults=config_defaults,
        )

class ValidationResultStore(NamespacedReadWriteStore):
    
    def _init_store_backend(self, store_backend_config, runtime_config):
        self.key_class = ValidationResultIdentifier

        if store_backend_config["class_name"] == "FixedLengthTupleFilesystemStoreBackend":
            config_defaults = {
                "key_length" : 5,
                "module_name" : "great_expectations.data_context.store",
            }
        else:
            config_defaults = {
                "module_name" : "great_expectations.data_context.store",
            }

        return instantiate_class_from_config(
            config=store_backend_config,
            runtime_config=runtime_config,
            config_defaults=config_defaults,
        )


from six import string_types
from great_expectations.data_context.types.base_resource_identifiers import (
    DataContextKey,
)

class SiteSectionResource(DataContextKey):
    _required_keys = set([
        "site_section_name",
        "resource_identifier",
    ])
    _allowed_keys = _required_keys
    _key_types = {
        "site_section_name" : string_types,
        # "resource_identifier", ... is NOT strictly typed.
    }

    def __hash__(self):
        return hash(self.site_section_name+"::"+self.resource_identifier.to_string())

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

class HtmlSiteStore(NamespacedReadWriteStore):

    def __init__(self,
        root_directory,
        base_directory,
        serialization_type=None,
    ):
        # Each key type gets its own backend.
        # If backends were DB connections, this could be inefficient, but it doesn't much matter for filepaths.
        # One thing to watch for is reversibility of keys.
        # If several types are being writtten to overlapping directories, we could get collisions.
        self.store_backends = {
            ExpectationSuiteIdentifier : instantiate_class_from_config(
                config = {
                    "module_name" : "great_expectations.data_context.store",
                    "class_name" : "FixedLengthTupleFilesystemStoreBackend",
                    "key_length" : 4,
                    "base_directory" : base_directory,
                    "filepath_template" : '{0}/{1}/{2}/{3}.html',
                },
                runtime_config={
                    "root_directory": root_directory
                }
            ),
            ValidationResultIdentifier : instantiate_class_from_config(
                config = {
                    "module_name" : "great_expectations.data_context.store",
                    "class_name" : "FixedLengthTupleFilesystemStoreBackend",
                    "key_length" : 5,
                    "base_directory" : base_directory,
                    "filepath_template" : '{4}/{0}/{1}/{2}/{3}.html',
                },
                runtime_config={
                    "root_directory": root_directory
                }
            ),
        }
        # self._init_store_backend(
        #     copy.deepcopy(store_backend),
        #     runtime_config={
        #         "root_directory": root_directory
        #     }
        # )
        self.root_directory = root_directory
        self.serialization_type = serialization_type
        self.base_directory = base_directory

        self.keys = set()

    # def _init_store_backend(self, store_backend_config, runtime_config):

    #     # filepath_template: '{0}/{1}/{2}/{3}.json'

    #     # if store_backend_config["class_name"] == "FixedLengthTupleFilesystemStoreBackend":
    #     #     config_defaults = {
    #     #         "key_length" : 4,
    #     #         "module_name" : "great_expectations.data_context.store",
    #     #     }
    #     # else:
    #     #     config_defaults = {
    #     #         "module_name" : "great_expectations.data_context.store",
    #     #     }

    #     return instantiate_class_from_config(
    #         config=store_backend_config,
    #         runtime_config=runtime_config,
    #         config_defaults={},
    #     )

    def _get(self, key):
        self._validate_key(key)

        # NOTE: We're currently not using the site_section_name of the key
        key_tuple = self._convert_resource_identifier_to_tuple(key.resource_identifier)
        return self.store_backends[
            type(key.resource_identifier)
        ].get(key_tuple)

    def _set(self, key, serialized_value):
        self._validate_key(key)

        self.keys.add(key)

        # NOTE: We're currently not using the site_section_name of the key
        key_tuple = self._convert_resource_identifier_to_tuple(key.resource_identifier)
        print(key_tuple)
        return self.store_backends[
            type(key.resource_identifier)
        ].set(key_tuple, serialized_value)

    def _validate_key(self, key):
        if not isinstance(key, SiteSectionResource):
            raise TypeError("key: {!r} must a SiteSectionResource, not {!r}".format(
                key,
                type(key),
            ))

        for key_class in self.store_backends.keys():
            if isinstance(key.resource_identifier, key_class):
                return

        # The key's resource_identifier didn't match any known key_class
        raise TypeError("resource_identifier in key: {!r} must one of {}, not {!r}".format(
            key,
            set(self.store_backends.keys()),
            type(key),
        ))

    def list_keys(self):
        return list(self.keys)

        keys = []
        for resource_identifier_type, store_backend in self.store_backends.items():
            print(store_backend.list_keys())
            keys += [resource_identifier_type(*key_resource_identifier) for key_resource_identifier in store_backend.list_keys()]

        return keys
