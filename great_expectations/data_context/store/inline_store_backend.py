from typing import Any, List, Optional

from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context.base_data_context import (
    BaseDataContext,
)
from great_expectations.data_context.store.store_backend import StoreBackend
from great_expectations.exceptions.exceptions import StoreBackendError
from great_expectations.util import filter_properties_dict


class InlineStoreBackend(StoreBackend):
    def __init__(
        self,
        data_context: BaseDataContext,
        runtime_environment: Optional[dict] = None,
        fixed_length_key: bool = False,
        suppress_store_backend_id: bool = False,
        manually_initialize_store_backend_id: str = "",
        store_name: Optional[str] = None,
    ) -> None:
        super().__init__(
            fixed_length_key=fixed_length_key,
            suppress_store_backend_id=suppress_store_backend_id,
            manually_initialize_store_backend_id=manually_initialize_store_backend_id,
            store_name=store_name,
        )

        self._data_context = data_context

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

    @property
    def config(self) -> dict:
        return self._config

    def _get(self, key) -> None:
        config_var: Any = self._data_context.config_variables[key]
        return config_var

    def _set(self, key, value, **kwargs) -> None:
        self._data_context.save_config_variable(
            config_variable_name=key, value=value, **kwargs
        )

    def _move(self, source_key, dest_key, **kwargs) -> None:
        raise StoreBackendError(
            "InlineStoreBackend does not support moving of keys; the DataContext's config variables schema is immutable"
        )

    def list_keys(self, prefix=()) -> List[str]:
        keys: List[str] = list(
            key for key in self._data_context.config_variables.keys()
        )
        return keys

    def remove_key(self, key) -> None:
        raise StoreBackendError(
            "InlineStoreBackend does not support moving of keys; the DataContext's config variables schema is immutable"
        )

    def _has_key(self, key) -> bool:
        return key in self._data_context.config_variables
