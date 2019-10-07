import logging

import copy
import os

from ..types.base_resource_identifiers import (
    DataContextKey,
)
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
    SiteSectionIdentifier,
)
from .store import (
    ReadWriteStore,
)
from .store_backend import (
    FixedLengthTupleStoreBackend
)
from great_expectations.data_context.util import (
    load_class,
    instantiate_class_from_config
)

logger = logging.getLogger(__name__)


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
        if store_backend_config["class_name"] == "FixedLengthTupleFilesystemStoreBackend":
            config_defaults = {
                "key_length" : 5,
                "module_name" : "great_expectations.data_context.store",
            }
        elif store_backend_config["class_name"] == "FixedLengthTupleS3StoreBackend":
            config_defaults = {
                "key_length": 5, # NOTE: Eugene: 2019-09-06: ???
                "module_name": "great_expectations.data_context.store",
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


class ExpectationsStore(NamespacedReadWriteStore):
    # Note : As of 2019/09/06, this method is untested.
    # It shares virtually all of its business logic with ValidationStore, which is under test.

    def _init_store_backend(self, store_backend_config, runtime_config):
        self.key_class = ExpectationSuiteIdentifier

        if store_backend_config["class_name"] == "FixedLengthTupleFilesystemStoreBackend":
            config_defaults = {
                "key_length": 4,
                "module_name": "great_expectations.data_context.store",
                "filepath_template": "{0}/{1}/{2}/{3}.json",
            }
        else:
            config_defaults = {
                "module_name": "great_expectations.data_context.store",
            }

        return instantiate_class_from_config(
            config=store_backend_config,
            runtime_config=runtime_config,
            config_defaults=config_defaults,
        )


class ValidationsStore(NamespacedReadWriteStore):
    
    def _init_store_backend(self, store_backend_config, runtime_config):
        self.key_class = ValidationResultIdentifier

        store_backend_class_name = store_backend_config.get("class_name", "FixedLengthTupleFilesystemStoreBackend")
        store_backend_module_name = store_backend_config.get("module_name", "great_expectations.data_context.store")
        store_backend_class = load_class(
            class_name=store_backend_class_name,
            module_name=store_backend_module_name
        )
        if issubclass(store_backend_class, FixedLengthTupleStoreBackend):
            config_defaults = {
                "key_length": 5,
                "module_name": "great_expectations.data_context.store",
                "filepath_template": "{4}/{0}/{1}/{2}/{3}.json"
            }
        else:
            config_defaults = {
                "module_name": "great_expectations.data_context.store",
            }

        return instantiate_class_from_config(
            config=store_backend_config,
            runtime_config=runtime_config,
            config_defaults=config_defaults,
        )


class HtmlSiteStore(NamespacedReadWriteStore):

    def __init__(self,
        root_directory,
        serialization_type=None,
        store_backend=None,
        **kwargs
    ):
        store_backend_module_name = store_backend.pop("module_name", "great_expectations.data_context.store")
        store_backend_class_name = store_backend.pop("class_name", "FixedLengthTupleFilesystemStoreBackend")
        store_class = load_class(store_backend_class_name, store_backend_module_name)
        if not isinstance(store_class, FixedLengthTupleStoreBackend):
            raise

        # Each key type gets its own backend.
        # If backends were DB connections, this could be inefficient, but it doesn't much matter for filepaths.
        # One thing to watch for is reversibility of keys.
        # If several types are being writtten to overlapping directories, we could get collisions.
        self.store_backends = {
            ExpectationSuiteIdentifier: instantiate_class_from_config(
                config = {
                    "module_name": store_backend_module_name,
                    "class_name": store_backend_class_name,
                    "key_length": 4,
                    "base_directory": base_directory,
                    "filepath_template": 'expectations/{0}/{1}/{2}/{3}.html',
                },
                runtime_config={
                    "root_directory": root_directory
                }
            ),
            ValidationResultIdentifier: instantiate_class_from_config(
                config = {
                    "module_name": store_backend_module_name,
                    "class_name": store_backend_class_name,
                    "key_length": 5,
                    "base_directory": base_directory,
                    "filepath_template": 'validations/{4}/{0}/{1}/{2}/{3}.html',
                },
                runtime_config={
                    "root_directory": root_directory
                }
            ),
        }

        self.root_directory = root_directory
        self.serialization_type = serialization_type
        self.base_directory = base_directory

        # NOTE: Instead of using the filesystem as the source of record for keys,
        # this class trackes keys separately in an internal set.
        # This means that keys are stored for a specific session, but can't be fetched after the original
        # HtmlSiteStore instance leaves scope.
        # Doing it this way allows us to prevent namespace collisions among keys while still having multiple
        # backends that write to the same directory structure.
        # It's a pretty reasonable way for HtmlSiteStore to do its job---you just ahve to remember that it
        # can't necessarily set and list_keys like most other Stores.
        self.keys = set()

    def _get(self, key):
        self._validate_key(key)

        key_tuple = self._convert_resource_identifier_to_tuple(key.resource_identifier)
        return self.store_backends[
            type(key.resource_identifier)
        ].get(key_tuple)

    def _set(self, key, serialized_value):
        self._validate_key(key)

        self.keys.add(key)

        key_tuple = self._convert_resource_identifier_to_tuple(key.resource_identifier)
        return self.store_backends[
            type(key.resource_identifier)
        ].set(key_tuple, serialized_value)

    def _validate_key(self, key):
        if not isinstance(key, SiteSectionIdentifier):
            raise TypeError("key: {!r} must a SiteSectionIdentifier, not {!r}".format(
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

    def write_index_page(self, page):
        # NOTE: This method is a temporary hack.
        # Status as of 2019/09/09: this method is backward compatible against the previous implementation of site_builder
        # However, it doesn't support backend pluggability---only implementation in a local filesystem.
        # Also, if/when we want to support index pages at multiple levels of nesting, we'll need to extend.
        # 
        # Properly speaking, what we need is a class of BackendStore that can accomodate this...
        # It's tricky with the current stores, sbecause the core get/set logic depends so strongly on fixed-length keys.
        index_page_path = os.path.join(self.root_directory, self.base_directory, "index.html")
        with open(index_page_path, "w") as file_:
            file_.write(page)
        return index_page_path
