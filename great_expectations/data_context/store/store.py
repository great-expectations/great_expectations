import json


class Store(object):
    """
    """

    def __init__(self, serialization_type=None):
        self.serialization_type = serialization_type

    def get(self, key, serialization_type=None):
        value = self._get(key)

        if serialization_type:
            deserialization_method = self._get_deserialization_method(serialization_type)
        else:
            deserialization_method = self._get_deserialization_method(self.serialization_type)
        deserialized_value = deserialization_method(value)
        return deserialized_value

    def set(self, key, value, serialization_type=None):
        if serialization_type:
            serialization_method = self._get_serialization_method(serialization_type)
        else:
            serialization_method = self._get_serialization_method(self.serialization_type)
        
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

    # def _init_from_config(self, config):
    #     raise NotImplementedError

    def _get(self, key):
        raise NotImplementedError

    def _set(self, key, value):
        raise NotImplementedError


class InMemoryStore(Store):
    """
    """

    def __init__(self, serialization_type=None):
        #TODO: use super here.
        self.serialization_type = serialization_type
        self.store = {}

    # def _init_from_config(self, config):
    #     if "serialization_type" in config:
    #         self.serialization_type = config["serialization_type"]
    #     else:
    #         self.serialization_type = None

    #     self.store = {}

    def _get(self, key):
        return self.store[key]

    def _set(self, key, value):
        self.store[key] = value

class LocalFileSystemStore(Store):
    """
    """

    def _get(self, key):
        raise NotImplementedError

    def _set(self, key, value):
        raise NotImplementedError

class S3Store(Store):
    """
    """

    def _get(self, key):
        raise NotImplementedError

    def _set(self, key, value):
        raise NotImplementedError
