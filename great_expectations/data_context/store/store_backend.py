import logging
from abc import ABCMeta, abstractmethod

from great_expectations.exceptions import StoreBackendError, StoreError

logger = logging.getLogger(__name__)


class StoreBackend(metaclass=ABCMeta):
    """A store backend acts as a key-value store that can accept tuples as keys, to abstract away
    reading and writing to a persistence layer.

    In general a StoreBackend implementation must provide implementations of:
      - _get
      - _set
      - list_keys
      - _has_key
    """

    IGNORED_FILES = [".ipynb_checkpoints"]

    def __init__(self, fixed_length_key=False):
        self._fixed_length_key = fixed_length_key

    @property
    def fixed_length_key(self):
        return self._fixed_length_key

    def get(self, key, **kwargs):
        self._validate_key(key)
        value = self._get(key, **kwargs)
        return value

    def set(self, key, value, **kwargs):
        self._validate_key(key)
        self._validate_value(value)
        # Allow the implementing setter to return something (e.g. a path used for its key)
        try:
            return self._set(key, value, **kwargs)
        except ValueError as e:
            logger.debug(str(e))
            raise StoreBackendError("ValueError while calling _set on store backend.")

    def move(self, source_key, dest_key, **kwargs):
        self._validate_key(source_key)
        self._validate_key(dest_key)
        return self._move(source_key, dest_key, **kwargs)

    def has_key(self, key):
        self._validate_key(key)
        return self._has_key(key)

    def get_url_for_key(self, key, protocol=None):
        raise StoreError(
            "Store backend of type {:s} does not have an implementation of get_url_for_key".format(
                type(self).__name__
            )
        )

    def _validate_key(self, key):
        if isinstance(key, tuple):
            for key_element in key:
                if not isinstance(key_element, str):
                    raise TypeError(
                        "Elements within tuples passed as keys to {} must be instances of {}, not {}".format(
                            self.__class__.__name__, str, type(key_element),
                        )
                    )
        else:
            raise TypeError(
                "Keys in {} must be instances of {}, not {}".format(
                    self.__class__.__name__, tuple, type(key),
                )
            )

    def _validate_value(self, value):
        pass

    @abstractmethod
    def _get(self, key):
        raise NotImplementedError

    @abstractmethod
    def _set(self, key, value, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def _move(self, source_key, dest_key, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def list_keys(self, prefix=()):
        raise NotImplementedError

    @abstractmethod
    def remove_key(self, key):
        raise NotImplementedError

    def _has_key(self, key):
        raise NotImplementedError

    def is_ignored_key(self, key):
        for ignored in self.IGNORED_FILES:
            if ignored in key:
                return True

        return False


class InMemoryStoreBackend(StoreBackend):
    """Uses an in-memory dictionary as a store backend.
    """

    # noinspection PyUnusedLocal
    def __init__(self, runtime_environment=None, fixed_length_key=False):
        super().__init__(fixed_length_key=fixed_length_key)
        self._store = {}

    def _get(self, key):
        return self._store[key]

    def _set(self, key, value, **kwargs):
        self._store[key] = value

    def _move(self, source_key, dest_key, **kwargs):
        self._store[dest_key] = self._store[source_key]
        self._store.pop(source_key)

    def list_keys(self, prefix=()):
        return [key for key in self._store.keys() if key[: len(prefix)] == prefix]

    def _has_key(self, key):
        return key in self._store

    def remove_key(self, key):
        del self._store[key]
