from typing import Optional

from great_expectations.core.data_context_key import DataContextVariableKey
from great_expectations.data_context.data_context_variables import (
    DataContextVariableSchema,
)
from great_expectations.data_context.store.store_backend import StoreBackend
from great_expectations.data_context.types.resource_identifiers import DataContextKey
from great_expectations.exceptions import InvalidKeyError
from great_expectations.util import filter_properties_dict


class InMemoryStoreBackend(StoreBackend):
    """Uses an in-memory dictionary as a store backend."""

    # noinspection PyUnusedLocal
    def __init__(  # noqa: PLR0913
        self,
        runtime_environment=None,
        fixed_length_key=False,
        suppress_store_backend_id=False,
        manually_initialize_store_backend_id: str = "",
        store_name=None,
    ) -> None:
        super().__init__(
            fixed_length_key=fixed_length_key,
            suppress_store_backend_id=suppress_store_backend_id,
            manually_initialize_store_backend_id=manually_initialize_store_backend_id,
            store_name=store_name,
        )
        self._store: dict = {}
        # Initialize with store_backend_id if not part of an HTMLSiteStore
        if not self._suppress_store_backend_id:
            _ = self.store_backend_id

        # Gather the call arguments of the present function (include the "module_name" and add the "class_name"), filter
        # out the Falsy values, and set the instance "_config" variable equal to the resulting dictionary.
        self._config = {
            "runtime_environment": runtime_environment,
            "fixed_length_key": fixed_length_key,
            "suppress_store_backend_id": suppress_store_backend_id,
            "manually_initialize_store_backend_id": manually_initialize_store_backend_id,
            "store_name": store_name,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)

    def _get(self, key):
        try:
            return self._store[key]
        except KeyError as e:
            raise InvalidKeyError(f"{str(e)}")

    def _set(self, key, value, **kwargs) -> None:
        self._store[key] = value

    def _move(self, source_key, dest_key, **kwargs) -> None:
        self._store[dest_key] = self._store[source_key]
        self._store.pop(source_key)

    def list_keys(self, prefix=()):
        return [key for key in self._store.keys() if key[: len(prefix)] == prefix]

    def _has_key(self, key):
        return key in self._store

    def remove_key(self, key) -> None:
        if isinstance(key, DataContextKey):
            key = key.to_tuple()
        del self._store[key]

    @property
    def config(self) -> dict:
        return self._config

    def build_key(  # type: ignore[override]
        self,
        resource_type: Optional[DataContextVariableSchema] = None,
        id: Optional[str] = None,
        name: Optional[str] = None,
    ) -> DataContextVariableKey:
        """Get the store backend specific implementation of the key. id included for super class compatibility."""
        return DataContextVariableKey(
            resource_name=name,
        )
