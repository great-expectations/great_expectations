class Store(object):
    """
    """

    def __init__(self, config=None):
        self._init_from_config(config)

    def get(self, key, deserialization_method=None):
        value = self._get(key)
        deserialization_method = self._get_deserialization_method(deserialization_method)
        deserialized_value = deserialization_method(value)
        return deserialized_value

    def set(self, key, value, serialization_method=None):
        serialization_method = self._get_serialization_method(serialization_method)
        serialized_value = serialization_method(value)
        self._set(key, serialized_value)

    def _get_serialization_method(self, serialization_method):
        if serialization_method == None:
            return lambda x: x

        #TODO:
        pass

    def _get_deserialization_method(self, deserialization_method):
        if deserialization_method == None:
            return lambda x: x

        #TODO:
        pass

    def _init_from_config(self, config):
        raise NotImplementedError

    def _get(self, key):
        raise NotImplementedError

    def _set(self, key, value):
        raise NotImplementedError


class InMemoryStore(Store):
    """
    """

    def _init_from_config(self, config):
        self.store = {}

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
