from typing import Any, List, Optional, Tuple

from great_expectations.data_context.data_context.data_context import DataContext
from great_expectations.data_context.store.store_backend import StoreBackend
from great_expectations.exceptions.exceptions import StoreBackendError
from great_expectations.util import filter_properties_dict


class InlineStoreBackend(StoreBackend):
    def __init__(
        self,
        data_context: DataContext,
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

    def _get(self, key: Tuple[str, ...]) -> Any:
        config_var: str = key[0]
        val: Any = self._data_context.project_config_with_variables_substituted[
            config_var
        ]
        return val

    def _set(self, key: Tuple[str, ...], value: Any, **kwargs: dict) -> None:
        config_var: str = key[0]
        self._data_context.config[config_var] = value
        self._data_context._save_project_config()

    def _move(
        self, source_key: Tuple[str, ...], dest_key: Tuple[str, ...], **kwargs: dict
    ) -> None:
        raise StoreBackendError(
            "InlineStoreBackend does not support moving of keys; the DataContext's config variables schema is immutable"
        )

    def list_keys(self, prefix: Tuple[str, ...] = ()) -> List[str]:
        """
        See `StoreBackend.list_keys` for more information.
        """
        keys: List[str] = list(key for key in self._data_context.config.to_dict())
        return keys

    def remove_key(self, key: Tuple[str, ...]) -> None:
        """
        Not relevant to this StoreBackend due to reliance on the DataContext but necessary to fulfill contract set by parent.
        See `StoreBackend.remove_key` for more information.
        """
        raise StoreBackendError(
            "InlineStoreBackend does not support the deletion of keys; the DataContext's config variables schema is immutable"
        )

    def _has_key(self, key: Tuple[str, ...]) -> bool:
        return key in self._data_context.config

    def _validate_key(self, key: Tuple[str, ...]) -> None:
        super()._validate_key(key)

        from great_expectations.data_context.types.base import DataContextConfig

        for attr in key:
            if attr not in self._data_context.config:
                raise TypeError(
                    f"Keys in {self.__class__.__name__} must adhere to the schema defined by {DataContextConfig.__name__}"
                )
