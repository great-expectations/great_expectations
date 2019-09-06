import logging
logger = logging.getLogger(__name__)

import importlib
from six import string_types
import copy
import json

import pandas as pd

from ...types import (
    ListOf,
    DotDict,
    AllowedKeysDotDict,
)
from ..types.base_resource_identifiers import (
    DataContextKey,
)
from ..types.resource_identifiers import (
    ValidationResultIdentifier,
)
from .store import (
    ReadWriteStore,
)
from great_expectations.data_context.util import (
    instantiate_class_from_config
)

class ValidationResultStore(ReadWriteStore):
    
    def __init__(self,
        store_backend,
        root_directory,
        serialization_type="json"
    ):
        self.store_backend = self._init_store_backend(
            store_backend,
            runtime_config={
                "root_directory": root_directory
            }
        )
        self.root_directory = root_directory
        self.serialization_type = serialization_type

    def _init_store_backend(self, store_backend_config, runtime_config):
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

    def _convert_resource_identifier_to_tuple(self, key):
        # TODO : Optionally prepend a source_id (the frontend Store name) to the tuple.

        # TODO : Optionally prepend a resource_identifier_type to the tuple.
        # list_ = [self.config.resource_identifier_class_name]

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
        new_identifier = ValidationResultIdentifier(*tuple_, coerce_type=True)#[1:]) #Only truncate one if we prepended the identifier type
        return new_identifier

    def _validate_key(self, key):
        if not isinstance(key, ValidationResultIdentifier):
            raise TypeError("key: {!r} must be a ValidationResultIdentifier, not {!r}".format(
                key,
                type(key),
            ))
