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
from ..types.resource_identifiers import (
    DataContextResourceIdentifier,
)


class WriteOnlyStoreConfig(AllowedKeysDotDict):
    _required_keys = set([
        "serialization_type"
    ])
    _allowed_keys = _required_keys


class WriteOnlyStore(object):

    # config_class = None

    # TODO : Refactor __init__s among this class and its children.
    def __init__(self, config, root_directory=None):
        assert hasattr(self, 'config_class')

        assert isinstance(config, self.config_class)
        self.config = config

        self.root_directory = root_directory
        # self.store_backend = self._configure_store_backend(self.config.store_backend)

        self._setup()

    def set(self, key, value, serialization_type=None):
        self._validate_key(key)
        
        if serialization_type:
            serialization_method = self._get_serialization_method(
                serialization_type)
        else:
            serialization_method = self._get_serialization_method(
                self.config.serialization_type)

        serialized_value = serialization_method(value)
        self._set(key, serialized_value)


    @classmethod
    def get_config_class(cls):
        return cls.config_class

    def _get_serialization_method(self, serialization_type):
        if serialization_type == None:
            return lambda x: x

        elif serialization_type == "json":
            return json.dumps

        elif serialization_type == "pandas_csv":

            def convert_to_csv(df):
                logger.debug("Starting convert_to_csv")

                assert isinstance(df, pd.DataFrame)

                return df.to_csv(index=None)

            return convert_to_csv

        # TODO: Add more serialization methods as needed

    def _validate_key(self, key):
        raise NotImplementedError

    def _validate_value(self, value):
        # NOTE : This is probably mainly a check of serializability using the chosen serialization_type.
        # Might be redundant.
        raise NotImplementedError

    def _setup(self):
        pass

    def _set(self, key, value):
        raise NotImplementedError

    def _configure_store_backend(self, store_backend_config):
        modified_store_backend_config = copy.deepcopy(store_backend_config)

        module_name = modified_store_backend_config.pop("module_name")
        class_name = modified_store_backend_config.pop("class_name")

        module = importlib.import_module(module_name)
        store_backend_class = getattr(module, class_name)

        self.store_backend = store_backend_class(
            modified_store_backend_config,
            self.root_directory
        )

        #For convenience when testing
        return self.store_backend



class ReadWriteStoreConfig(WriteOnlyStoreConfig):
    pass


class ReadWriteStore(WriteOnlyStore):

    def get(self, key, serialization_type=None):
        self._validate_key(key)

        value=self._get(key)

        if serialization_type:
            deserialization_method = self._get_deserialization_method(
                serialization_type)
        else:
            deserialization_method = self._get_deserialization_method(
                self.config.serialization_type)
        deserialized_value = deserialization_method(value)
        return deserialized_value

    def _get(self, key):
        raise NotImplementedError

    def list_keys(self):
        raise NotImplementedError

    def has_key(self, key):
        raise NotImplementedError

    def _get_deserialization_method(self, serialization_type):
        if serialization_type == None:
            return lambda x: x

        elif serialization_type == "json":
            return json.loads

        elif serialization_type == "pandas_csv":
            # TODO:
            raise NotImplementedError

        # TODO: Add more serialization methods as needed


class BasicInMemoryStoreConfig(ReadWriteStoreConfig):
    _allowed_keys = set([
        "serialization_type"
    ]) #ReadWriteStoreConfig._allowed_keys
    _required_keys = set([]) #ReadWriteStoreConfig._required_keys

class BasicInMemoryStore(ReadWriteStore):
    """Like a dict, but much harder to write.
    
    This class uses an InMemoryStoreBackend, but I question whether it's worth it.
    It would be easier just to wrap a dict.
    """

    config_class = BasicInMemoryStoreConfig

    def __init__(self, config=None, root_directory=None):
        assert hasattr(self, 'config_class')

        if config == None:
            config = BasicInMemoryStoreConfig(**{
                "serialization_type": None,
            })

        assert isinstance(config, self.config_class)
        self.config = config

        self.root_directory = root_directory

        self._setup()

    def _setup(self):
        self.store_backend = self._configure_store_backend({
            "module_name" : "great_expectations.data_context.store",
            "class_name" : "InMemoryStoreBackend",
            "separator" : ".",
        })

        print(self.store_backend)
        print(self.store_backend.store)

    def _validate_key(self, key):
        assert isinstance(key, string_types)

    def _get(self, key):
        return self.store_backend.get((key,))
    
    def _set(self, key, value):
        self.store_backend.set((key,), value)

    def has_key(self, key):
        return self.store_backend.has_key((key,))

    def list_keys(self):
        return [key for key, in self.store_backend.list_keys()]


class NamespacedReadWriteStoreConfig(ReadWriteStoreConfig):
    _allowed_keys = set({
        "serialization_type",
        "resource_identifier_class_name",
        "store_backend",
    })
    _required_keys = set({
        "resource_identifier_class_name",
        "store_backend",
    })


class NamespacedReadWriteStore(ReadWriteStore):
    
    config_class = NamespacedReadWriteStoreConfig

    def __init__(self, config, root_directory):
        # super(NamespacedReadWriteStore, self).__init__(config, root_directory)

        # TODO: This method was copied and modified from the base class. 
        # We need to refactor later to inherit sensibly.
        assert hasattr(self, 'config_class')

        assert isinstance(config, self.config_class)
        self.config = config

        self.root_directory = root_directory

        # NOTE: hm. This is tricky.
        # At this point, we need to add some keys to the store_backend config.
        # The config from THIS class should be typed by this point.
        # But if we insist that it's recursively typed, it will have failed before arriving at this point.
        if self.config["store_backend"]["class_name"] == "FilesystemStoreBackend":
            self.config["store_backend"]["key_length"] = self.resource_identifier_class._recursively_get_key_length()#+1 #Only add one if we prepend the identifier type
            self.store_backend = self._configure_store_backend(self.config["store_backend"])
            self.store_backend.verify_that_key_to_filepath_operation_is_reversible()

        else:
            self.store_backend = self._configure_store_backend(self.config["store_backend"])
    

        self._setup()


    def _get(self, key):
        key_tuple = self._convert_resource_identifier_to_tuple(key)
        return self.store_backend.get(key_tuple)

    def _set(self, key, serialized_value):
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

        for _, key_element in key.items():
            if isinstance( key_element, DataContextResourceIdentifier ):
                list_ += self._convert_resource_identifier_to_list(key_element)
            else:
                list_.append(key_element)

        return list_

    def _convert_tuple_to_resource_identifier(self, tuple_):
        new_identifier = self.resource_identifier_class(*tuple_)#[1:]) #Only truncate one if we prepended the identifier type
        return new_identifier

    @property
    def resource_identifier_class(self):
        module = importlib.import_module("great_expectations.data_context.types.resource_identifiers")
        class_ = getattr(module, self.config.resource_identifier_class_name)
        return class_

    def _validate_key(self, key):
        if not isinstance(key, self.resource_identifier_class):
            raise TypeError("key: {!r} must be a DataContextResourceIdentifier, not {!r}".format(
                key,
                type(key),
            ))


class EmptyConfig(DotDict):
    pass

class EvaluationParameterStore(object):
    """Fine. You want to be a dict. You get to be a dict.
    
    TODO: Refactor this into a true Store later.

    It would be easy to replace all instances of EvaluationParameterStore, except that Stores currently insist on having string_typed values,
    and the the DataContext rudely sends dictionaries.

    On reflection, there's no reason for all Stores to insist on serializability.
    """

    config_class = EmptyConfig

    def __init__(self, config=None, root_directory=None):
        self.store = {}

    @classmethod
    def get_config_class(cls):
        return cls.config_class

    def get(self, key):
        return self.store[key]

    def set(self, key, value):
        self.store[key] = value

    def has_key(self, key):
        return key in self.store
