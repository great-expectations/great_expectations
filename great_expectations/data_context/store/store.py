from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type

from typing_extensions import TypedDict

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.data_context_key import DataContextKey
from great_expectations.data_context.store.gx_cloud_store_backend import (
    GXCloudStoreBackend,
)
from great_expectations.data_context.store.store_backend import StoreBackend
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GXCloudIdentifier,
)
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.exceptions import ClassInstantiationError, DataContextError

if TYPE_CHECKING:
    # min version of typing_extension missing `NotRequired`, so it can't be imported at runtime
    from typing_extensions import NotRequired

    from great_expectations.core.configuration import AbstractConfig

logger = logging.getLogger(__name__)


class StoreConfigTypedDict(TypedDict):
    # NOTE: TypeDict values may be incomplete, update as needed
    class_name: str
    module_name: NotRequired[str]
    store_backend: dict


class DataDocsSiteConfigTypedDict(TypedDict):
    # NOTE: TypeDict values may be incomplete, update as needed
    class_name: str
    module_name: NotRequired[str]
    store_backend: dict
    site_index_builder: dict


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

    def __init__(
        self,
        store_backend: Optional[dict] = None,
        runtime_environment: Optional[dict] = None,
        store_name: str = "no_store_name",
    ) -> None:
        """
        Runtime environment may be necessary to instantiate store backend elements.
        Args:
            store_backend:
            runtime_environment:
            store_name: store name given in the DataContextConfig (via either in-code or yaml configuration)
        """
        if store_backend is None:
            store_backend = {"class_name": "InMemoryStoreBackend"}
        self._store_name = store_name
        logger.debug("Building store_backend.")
        module_name = "great_expectations.data_context.store"
        self._store_backend = instantiate_class_from_config(
            config=store_backend,
            runtime_environment=runtime_environment or {},
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
        """
        This method takes full json response from GX cloud and outputs a dict appropriate for
        deserialization into a GX object
        """
        return response_json

    def _validate_key(self, key: DataContextKey) -> None:
        # STORE_BACKEND_ID_KEY always validated
        if key == StoreBackend.STORE_BACKEND_ID_KEY:
            return
        elif isinstance(key, self.key_class):
            return
        else:
            raise TypeError(
                f"key must be an instance of {self.key_class.__name__}, not {type(key)}"
            )

    @property
    def cloud_mode(self) -> bool:
        return isinstance(self._store_backend, GXCloudStoreBackend)

    @property
    def ge_cloud_mode(self) -> bool:
        # <GX_RENAME> Deprecated 0.15.37
        return self.cloud_mode

    @property
    def store_backend(self) -> StoreBackend:
        return self._store_backend

    @property
    def store_name(self) -> str:
        return self._store_name

    @property
    def store_backend_id(self) -> str:
        """
        Report the store_backend_id of the currently-configured StoreBackend
        Returns:
            store_backend_id which is a UUID(version=4)
        """
        return self._store_backend.store_backend_id

    @property
    def key_class(self) -> Type[DataContextKey]:
        if self.cloud_mode:
            return GXCloudIdentifier
        return self._key_class

    @property
    def store_backend_id_warnings_suppressed(self) -> str:
        """
        Report the store_backend_id of the currently-configured StoreBackend, suppressing warnings for invalid configurations.
        Returns:
            store_backend_id which is a UUID(version=4)
        """
        return self._store_backend.store_backend_id_warnings_suppressed

    @property
    def config(self) -> dict:
        raise NotImplementedError

    # noinspection PyMethodMayBeStatic
    def serialize(self, value: Any) -> Any:
        return value

    # noinspection PyMethodMayBeStatic
    def key_to_tuple(self, key: DataContextKey) -> Tuple[str, ...]:
        if self._use_fixed_length_key:
            return key.to_fixed_length_tuple()
        return key.to_tuple()

    def tuple_to_key(self, tuple_: Tuple[str, ...]) -> DataContextKey:
        if tuple_ == StoreBackend.STORE_BACKEND_ID_KEY:
            return StoreBackend.STORE_BACKEND_ID_KEY[0]  # type: ignore[return-value]
        if self._use_fixed_length_key:
            return self.key_class.from_fixed_length_tuple(tuple_)
        return self.key_class.from_tuple(tuple_)

    # noinspection PyMethodMayBeStatic
    def deserialize(self, value: Any) -> Any:
        return value

    def get(
        self, key: DataContextKey | GXCloudIdentifier | ConfigurationIdentifier
    ) -> Optional[Any]:
        if key == StoreBackend.STORE_BACKEND_ID_KEY:
            return self._store_backend.get(key)

        if self.cloud_mode:
            self._validate_key(key)
            value = self._store_backend.get(self.key_to_tuple(key))
            # TODO [Robby] MER-285: Handle non-200 http errors
            if value:
                value = self.ge_cloud_response_json_to_object_dict(response_json=value)
        else:
            self._validate_key(key)
            value = self._store_backend.get(self.key_to_tuple(key))

        if value:
            return self.deserialize(value)

        return None

    def set(self, key: DataContextKey, value: Any, **kwargs) -> None:
        if key == StoreBackend.STORE_BACKEND_ID_KEY:
            return self._store_backend.set(key, value, **kwargs)

        self._validate_key(key)
        return self._store_backend.set(
            self.key_to_tuple(key), self.serialize(value), **kwargs
        )

    def add(self, key: DataContextKey, value: Any, **kwargs) -> None:
        """
        Essentially `set` but validates that a given key-value pair does not already exist.
        """
        return self._add(key=key, value=value, **kwargs)

    def _add(self, key: DataContextKey, value: Any, **kwargs) -> None:
        self._validate_key(key)
        return self._store_backend.add(
            self.key_to_tuple(key), self.serialize(value), **kwargs
        )

    def update(self, key: DataContextKey, value: Any, **kwargs) -> None:
        """
        Essentially `set` but validates that a given key-value pair does already exist.
        """
        return self._update(key=key, value=value, **kwargs)

    def _update(self, key: DataContextKey, value: Any, **kwargs) -> None:
        self._validate_key(key)
        return self._store_backend.update(
            self.key_to_tuple(key), self.serialize(value), **kwargs
        )

    def add_or_update(self, key: DataContextKey, value: Any, **kwargs) -> None:
        """
        Conditionally calls `add` or `update` based on the presence of the given key.
        """
        return self._add_or_update(key=key, value=value, **kwargs)

    def _add_or_update(self, key: DataContextKey, value: Any, **kwargs) -> None:
        self._validate_key(key)
        return self._store_backend.add_or_update(
            self.key_to_tuple(key), self.serialize(value), **kwargs
        )

    def list_keys(self) -> List[DataContextKey]:
        keys_without_store_backend_id = [
            key
            for key in self._store_backend.list_keys()
            if not key == StoreBackend.STORE_BACKEND_ID_KEY
        ]
        return [self.tuple_to_key(key) for key in keys_without_store_backend_id]

    def has_key(self, key: DataContextKey) -> bool:
        if key == StoreBackend.STORE_BACKEND_ID_KEY:
            return self._store_backend.has_key(key)
        else:
            if self._use_fixed_length_key:
                return self._store_backend.has_key(key.to_fixed_length_tuple())
            return self._store_backend.has_key(key.to_tuple())

    def self_check(self, pretty_print: bool) -> None:
        NotImplementedError(
            f"The test method is not implemented for Store class {self.__class__.__name__}."
        )

    def _build_key_from_config(self, config: AbstractConfig) -> DataContextKey:
        id: Optional[str] = None
        # Chetan - 20220831 - Explicit fork in logic to cover legacy behavior (particularly around Checkpoints).
        # Will be removed as part of the effort to rename `ge_cloud_id` to `id` across the codebase.
        if hasattr(config, "ge_cloud_id"):
            id = config.ge_cloud_id
        if hasattr(config, "id"):
            id = config.id

        name: Optional[str] = None
        if hasattr(config, "name"):
            name = config.name

        return self.store_backend.build_key(name=name, id=id)

    @staticmethod
    def build_store_from_config(
        store_name: Optional[str] = None,
        store_config: StoreConfigTypedDict | dict | None = None,
        module_name: str = "great_expectations.data_context.store",
        runtime_environment: Optional[dict] = None,
    ) -> Store:
        if store_config is None or module_name is None:
            raise gx_exceptions.StoreConfigurationError(
                "Cannot build a store without both a store_config and a module_name"
            )

        try:
            config_defaults: dict = {
                "store_name": store_name,
                "module_name": module_name,
            }
            new_store = instantiate_class_from_config(
                config=store_config,
                runtime_environment=runtime_environment,
                config_defaults=config_defaults,
            )
        except gx_exceptions.DataContextError as e:
            logger.critical(
                f"Error {e} occurred while attempting to instantiate a store."
            )
            class_name: str = store_config["class_name"]
            module_name = store_config.get("module_name", module_name)
            raise gx_exceptions.ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=class_name,
            ) from e

        return new_store
