from typing import Any, List, Optional, Tuple

from great_expectations.data_context.store.store_backend import StoreBackend
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context.types.data_context_variables import (
    DataContextVariableSchema,
)
from great_expectations.exceptions.exceptions import StoreBackendError
from great_expectations.util import filter_properties_dict


class InlineStoreBackend(StoreBackend):
    """
    The InlineStoreBackend enables CRUD behavior with the fields noted in a user's project config (`great_expectations.yml`).

    It performs these actions through a reference to a DataContext instance.
    Please note that is it only to be used with file-backed DataContexts (DataContext and FileDataContext).
    """

    def __init__(
        self,
        data_context: "DataContext",  # noqa: F821
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

    def _get(self, key: Tuple[DataContextVariableSchema, str]) -> Any:
        config_var_type: DataContextVariableSchema = key[0]
        config_var_name: Optional[str] = key[1]

        variable_config: Any = self._data_context.config[config_var_type]

        if config_var_name:
            return variable_config[config_var_name]
        return variable_config

    def _set(
        self, key: Tuple[DataContextVariableSchema, str], value: Any, **kwargs: dict
    ) -> None:
        config_var_type: DataContextVariableSchema = key[0]
        config_var_name: Optional[str] = key[1]

        project_config: DataContextConfig = self._data_context.config

        if config_var_name:
            project_config[config_var_type][config_var_name] = value
        else:
            project_config[config_var_type] = value

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
        if len(key) != 2:
            raise TypeError(
                f"Keys used in {self.__class__.__name__} must be composed of two parts: a variable type and an optional name"
            )

        type_, _ = key

        if not isinstance(
            type_, DataContextVariableSchema
        ) and not DataContextVariableSchema.has_value(type_):
            raise TypeError(
                f"Keys in {self.__class__.__name__} must adhere to the schema defined by {DataContextVariableSchema.__name__}; invalid type {type(type_)} found"
            )
