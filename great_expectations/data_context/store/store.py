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
from great_expectations.data_context.util import (
    instantiate_class_from_config
)

# TODO : Add a ConfigReadWriteStore.

class WriteOnlyStore(object):
    """This base class supports writing, but not reading.

    It's suitable for things like HTML files that are information sinks.
    """

    def __init__(self, serialization_type=None, root_directory=None):
        self.serialization_type = serialization_type
        self.root_directory = root_directory

    def set(self, key, value, serialization_type=None):
        self._validate_key(key)
        
        if serialization_type:
            serialization_method = self._get_serialization_method(
                serialization_type)
        else:
            serialization_method = self._get_serialization_method(
                self.serialization_type)

        serialized_value = serialization_method(value)
        return self._set(key, serialized_value)


    # NOTE : Abe 2019/09/06 : It's unclear whether this serialization logic belongs here,
    # or should be factored out to individual classes on a case-by-case basis.

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


class ReadWriteStore(WriteOnlyStore):
    """This base class supports both reading and writing.

    Most of the core objects in DataContext are handled by subclasses of ReadWriteStore.
    """

    def get(self, key, serialization_type=None):
        self._validate_key(key)

        value=self._get(key)

        if serialization_type:
            deserialization_method = self._get_deserialization_method(
                serialization_type)
        else:
            deserialization_method = self._get_deserialization_method(
                self.serialization_type)

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

class BasicInMemoryStore(ReadWriteStore):
    """Like a dict, but much harder to write.
    
    This class uses an InMemoryStoreBackend, but I question whether it's worth it.
    It would be easier just to wrap a dict.

    This class is used for testing and not much else.
    """

    def __init__(self, serialization_type=None, root_directory=None):
        self.serialization_type = serialization_type
        self.root_directory = root_directory

        self.store_backend = instantiate_class_from_config(
            config={
                "module_name" : "great_expectations.data_context.store",
                "class_name" : "InMemoryStoreBackend",
                "separator" : ".",
            },
            runtime_config={
                "root_directory": root_directory,
            },
            config_defaults={},
        )

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
