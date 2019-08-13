import json

from .store import (
    Store,
)

class JsonSerializableStore(Store):
    """
    """

    def read(self, key):
        unserialized_value = self._get(key)
        serialized_value = json.load()

    def write(self, key, value):
        serialized_value = json.dumps(value)
        self._write()

    def _read(self, key):
        raise NotImplementedError

    def _write(self, key, serialized_value):
        raise NotImplementedError


class JsonSerializableInMemoryStore(JsonSerializableStore):
    """
    """

    def _read(self, key):
        raise NotImplementedError

    def _write(self, key, serialized_value):
        raise NotImplementedError

class JsonSerializableFileStore(JsonSerializableStore):
    """
    """

    def _read(self, key):
        raise NotImplementedError

    def _write(self, key, serialized_value):
        raise NotImplementedError

class JsonSerializableS3Store(JsonSerializableStore):
    """
    """

    def _read(self, key):
        raise NotImplementedError

    def _write(self, key, serialized_value):
        raise NotImplementedError
