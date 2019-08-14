import json
import os

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

        #TODO:
        pass

    def _get_deserialization_method(self, serialization_type):
        if serialization_type == None:
            return lambda x: x

        elif serialization_type == "json":
            return json.loads

        #TODO:
        pass

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
        # super(DataContextAwareStore, self).__init__(
        #     serialization_type=serialization_type,
        # )

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
            # raise TypeError("config must be an instance of type {0}, not {1}".format(
            #     self.get_config_class(),
            #     type(config),
            # ))

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

    # def __init__(
    #     self,
    #     data_context,
    #     serialization_type=None
    # ):
    #     super(InMemoryStore, self).__init__(
    #         data_context=data_context,
    #         serialization_type=serialization_type,
    #     )

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
    # def __init__(
    #     self,
    #     data_context,
    #     base_directory,
    #     serialization_type=None,
    # ):
    #     super(FilesystemStore, self).__init__(
    #         data_context=data_context,
    #         serialization_type=serialization_type,
    #     )
        
    #     self.base_directory = base_directory
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

# class S3Store(DataContextAwareStore):
#     """Uses an S3 bucket+prefix as a store
#     """

#     def _get(self, key):
#         raise NotImplementedError

#     def _set(self, key, value):
#         raise NotImplementedError
