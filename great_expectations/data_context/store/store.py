import json
import os
import io

from ..util import safe_mmkdir
# from ..data_context import (
#     DataContext
# )
from .types import (
    InMemoryStoreConfig,
    FilesystemStoreConfig,
)

class Store(object):
    """A simple key-value store that supports getting and setting.

    Stores also support the concept of serialization.

    See tests/data_context/test_store.py for examples.
    """

    def __init__(
        self,
        serialization_type=None
    ):
        self.serialization_type = serialization_type

    def get(self, key, serialization_type=None):
        value = self._get(key)

        if serialization_type:
            deserialization_method = self._get_deserialization_method(serialization_type)
        else:
            deserialization_method = self._get_deserialization_method(self.config.serialization_type)
        deserialized_value = deserialization_method(value)
        return deserialized_value

    def set(self, key, value, serialization_type=None):
        if serialization_type:
            serialization_method = self._get_serialization_method(serialization_type)
        else:
            serialization_method = self._get_serialization_method(self.config.serialization_type)
        
        serialized_value = serialization_method(value)
        self._set(key, serialized_value)

    def _get_serialization_method(self, serialization_type):
        if serialization_type == None:
            return lambda x: x

        elif serialization_type == "json":
            return json.dumps

        elif serialization_type == "pandas_csv":
            #!!! This is a fast, janky, untested implementation
            def convert_to_csv(df):
                s_buf = io.StringIO()
                df.to_csv(s_buf)
                return s_buf.read()

            return convert_to_csv

        #TODO: Add more serialization methods as needed

    def _get_deserialization_method(self, serialization_type):
        if serialization_type == None:
            return lambda x: x

        elif serialization_type == "json":
            return json.loads

        elif serialization_type == "pandas_csv":
            #TODO:
            raise NotImplementedError

        #TODO: Add more serialization methods as needed

    def _get(self, key):
        raise NotImplementedError

    def _set(self, key, value):
        raise NotImplementedError

class DataContextAwareStore(Store):
    def __init__(
        self,
        data_context,
        config,
    ):
        #FIXME: Eek. This causes circular imports. What to do?        
        # if not isinstance(data_context, DataContext):
        #     raise TypeError("data_context must be an instance of type DataContext")

        self.data_context = data_context

        if not isinstance(config, self.get_config_class()):
            #Attempt to coerce config to a typed config
            config = self.get_config_class()(
                coerce_types=True,
                **config
            )

        self.config = config

        self._setup()

    def get(self, key, serialization_type=None):
        namespaced_key = self._get_namespaced_key(key)

        return super(DataContextAwareStore, self).get(
            namespaced_key,
            serialization_type=serialization_type,
        )
    
    def set(self, key, value, serialization_type=None):
        namespaced_key = self._get_namespaced_key(key)

        super(DataContextAwareStore, self).set(
            namespaced_key,
            value,
            serialization_type=serialization_type,
        )

    @classmethod
    def get_config_class(cls):
        return cls.config_class
    
    def _get_namespaced_key(self, key):
        """This method should be overridden for each subclass"""
        return key


class InMemoryStore(DataContextAwareStore):
    """Uses an in-memory dictionary as a store.
    """

    config_class = InMemoryStoreConfig

    def _setup(self):
        self.store = {}

    def _get(self, key):
        return self.store[key]

    def _set(self, key, value):
        self.store[key] = value


class FilesystemStore(DataContextAwareStore):
    """Uses a local filepath as a store.
    """

    config_class = FilesystemStoreConfig

    def _setup(self):
        safe_mmkdir(str(os.path.dirname(self.config.base_directory)))

    def _get(self, key):
        with open(os.path.join(self.config.base_directory, key)) as infile:
            return infile.read()

    def _set(self, key, value):
        filename = os.path.join(self.config.base_directory, key)
        safe_mmkdir(str(os.path.split(filename)[0]))
        with open(filename, "w") as outfile:
            outfile.write(value)

    def _get_namespaced_key(self, key):
        filepath = self.data_context._get_normalized_data_asset_name_filepath(
            key.normalized_data_asset_name,
            key.expectation_suite_name,
            base_path=os.path.join(
                self.data_context.root_directory,
                self.config.base_directory,
                key.run_id
            ),
            file_extension=self.config.file_extension
        )
        return filepath


# class S3Store(DataContextAwareStore):
#     """Uses an S3 bucket+prefix as a store
#     """

#     def _get(self, key):
#         raise NotImplementedError

#     def _set(self, key, value):
#         raise NotImplementedError
