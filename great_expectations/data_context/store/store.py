import logging

from great_expectations.core.data_context_key import DataContextKey
from great_expectations.data_context.store.store_backend import StoreBackend
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.exceptions import ClassInstantiationError, DataContextError

logger = logging.getLogger(__name__)


class Store:
    """A store is responsible for reading and writing Great Expectations objects
    to appropriate backends. It provides a generic API that the DataContext can
    use independently of any particular ORM and backend.

    An implementation of a store will generally need to define the following:
      - serialize
      - deserialize
      - _key_class (class of expected key type)

    All keys must have a to_tuple() method.
    """

    _key_class = DataContextKey

    def __init__(self, store_backend=None, runtime_environment=None):
        """Runtime environment may be necessary to instantiate store backend elements."""
        if store_backend is None:
            store_backend = {"class_name": "InMemoryStoreBackend"}
        logger.debug("Building store_backend.")
        module_name = "great_expectations.data_context.store"
        self._store_backend = instantiate_class_from_config(
            config=store_backend,
            runtime_environment=runtime_environment or {},
            config_defaults={"module_name": module_name},
        )
        if not self._store_backend:
            raise ClassInstantiationError(
                module_name=module_name, package_name=None, class_name=store_backend
            )
        if not isinstance(self._store_backend, StoreBackend):
            raise DataContextError(
                "Invalid StoreBackend configuration: expected a StoreBackend instance."
            )
        self._use_fixed_length_key = self._store_backend.fixed_length_key

    def _validate_key(self, key):
        if not isinstance(key, self._key_class):
            raise TypeError(
                "key must be an instance of %s, not %s"
                % (self._key_class.__name__, type(key))
            )

    @property
    def store_backend(self):
        return self._store_backend

    # noinspection PyMethodMayBeStatic
    def serialize(self, key, value):
        return value

    # noinspection PyMethodMayBeStatic
    def key_to_tuple(self, key):
        if self._use_fixed_length_key:
            return key.to_fixed_length_tuple()
        return key.to_tuple()

    def tuple_to_key(self, tuple_):
        if self._use_fixed_length_key:
            return self._key_class.from_fixed_length_tuple(tuple_)
        return self._key_class.from_tuple(tuple_)

    # noinspection PyMethodMayBeStatic
    def deserialize(self, key, value):
        return value

    def get(self, key):
        self._validate_key(key)
        value = self._store_backend.get(self.key_to_tuple(key))
        if value:
            return self.deserialize(key, value)

    def set(self, key, value):
        self._validate_key(key)
        return self._store_backend.set(
            self.key_to_tuple(key), self.serialize(key, value)
        )

    def list_keys(self):
        return [self.tuple_to_key(key) for key in self._store_backend.list_keys()]

    def has_key(self, key):
        if self._use_fixed_length_key:
            return self._store_backend.has_key(key.to_fixed_length_tuple())
        return self._store_backend.has_key(key.to_tuple())
