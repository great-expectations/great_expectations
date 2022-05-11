import logging
from typing import Dict

from great_expectations.core.data_context_key import DataContextKey
from great_expectations.data_context.store.ge_cloud_store_backend import (
    GeCloudStoreBackend,
)
from great_expectations.data_context.store.store_backend import StoreBackend
from great_expectations.data_context.types.resource_identifiers import GeCloudIdentifier
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.exceptions import ClassInstantiationError, DataContextError

logger = logging.getLogger(__name__)


class Store:
    "A store is responsible for reading and writing Great Expectations objects\n    to appropriate backends. It provides a generic API that the DataContext can\n    use independently of any particular ORM and backend.\n\n    An implementation of a store will generally need to define the following:\n      - serialize\n      - deserialize\n      - _key_class (class of expected key type)\n\n    All keys must have a to_tuple() method.\n"
    _key_class = DataContextKey

    def __init__(
        self, store_backend=None, runtime_environment=None, store_name="no_store_name"
    ) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Runtime environment may be necessary to instantiate store backend elements.\n        Args:\n            store_backend:\n            runtime_environment:\n            store_name: store name given in the DataContextConfig (via either in-code or yaml configuration)\n        "
        if store_backend is None:
            store_backend = {"class_name": "InMemoryStoreBackend"}
        self._store_name = store_name
        logger.debug("Building store_backend.")
        module_name = "great_expectations.data_context.store"
        self._store_backend = instantiate_class_from_config(
            config=store_backend,
            runtime_environment=(runtime_environment or {}),
            config_defaults={
                "module_name": module_name,
                "store_name": self._store_name,
            },
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

    def ge_cloud_response_json_to_object_dict(self, response_json: Dict) -> Dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        This method takes full json response from GE cloud and outputs a dict appropriate for\n        deserialization into a GE object\n        "
        return response_json

    def _validate_key(self, key):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if key == StoreBackend.STORE_BACKEND_ID_KEY:
            return
        elif not isinstance(key, self.key_class):
            raise TypeError(
                f"key must be an instance of {self.key_class.__name__}, not {type(key)}"
            )

    @property
    def ge_cloud_mode(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return isinstance(self._store_backend, GeCloudStoreBackend)

    @property
    def store_backend(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._store_backend

    @property
    def store_name(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return self._store_name

    @property
    def store_backend_id(self) -> str:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Report the store_backend_id of the currently-configured StoreBackend\n        Returns:\n            store_backend_id which is a UUID(version=4)\n        "
        return self._store_backend.store_backend_id

    @property
    def key_class(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if self.ge_cloud_mode:
            return GeCloudIdentifier
        return self._key_class

    @property
    def store_backend_id_warnings_suppressed(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        "\n        Report the store_backend_id of the currently-configured StoreBackend, suppressing warnings for invalid configurations.\n        Returns:\n            store_backend_id which is a UUID(version=4)\n        "
        return self._store_backend.store_backend_id_warnings_suppressed

    def serialize(self, key, value):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return value

    def key_to_tuple(self, key):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if self._use_fixed_length_key:
            return key.to_fixed_length_tuple()
        return key.to_tuple()

    def tuple_to_key(self, tuple_):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if tuple_ == StoreBackend.STORE_BACKEND_ID_KEY:
            return StoreBackend.STORE_BACKEND_ID_KEY[0]
        if self._use_fixed_length_key:
            return self.key_class.from_fixed_length_tuple(tuple_)
        return self.key_class.from_tuple(tuple_)

    def deserialize(self, key, value):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        return value

    def get(self, key):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if key == StoreBackend.STORE_BACKEND_ID_KEY:
            return self._store_backend.get(key)
        elif self.ge_cloud_mode:
            self._validate_key(key)
            value = self._store_backend.get(self.key_to_tuple(key))
            if value:
                value = self.ge_cloud_response_json_to_object_dict(response_json=value)
        else:
            self._validate_key(key)
            value = self._store_backend.get(self.key_to_tuple(key))
        if value:
            return self.deserialize(key, value)
        else:
            return None

    def set(self, key, value, **kwargs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if key == StoreBackend.STORE_BACKEND_ID_KEY:
            return self._store_backend.set(key, value, **kwargs)
        else:
            self._validate_key(key)
            return self._store_backend.set(
                self.key_to_tuple(key), self.serialize(key, value), **kwargs
            )

    def list_keys(self):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        keys_without_store_backend_id = [
            key
            for key in self._store_backend.list_keys()
            if (not (key == StoreBackend.STORE_BACKEND_ID_KEY))
        ]
        return [self.tuple_to_key(key) for key in keys_without_store_backend_id]

    def has_key(self, key):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        if key == StoreBackend.STORE_BACKEND_ID_KEY:
            return self._store_backend.has_key(key)
        else:
            if self._use_fixed_length_key:
                return self._store_backend.has_key(key.to_fixed_length_tuple())
            return self._store_backend.has_key(key.to_tuple())

    def self_check(self, pretty_print) -> None:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        NotImplementedError(
            f"The test method is not implemented for Store class {self.__class__.__name__}."
        )

    @property
    def config(self) -> dict:
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        raise NotImplementedError
