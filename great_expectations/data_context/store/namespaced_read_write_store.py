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

class HtmlSiteStore(NamespacedReadWriteStore):
    
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

