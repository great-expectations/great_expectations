from typing import Optional

from great_expectations.core.data_context_key import StringKey
from great_expectations.data_context.store.store import Store
from great_expectations.util import filter_properties_dict


class VariablesStore(Store):
    """
    A VariablesStore manages config variables for the DataContext.
    """

    _key_class = StringKey

    def __init__(
        self,
        store_name: Optional[str] = None,
        store_backend: Optional[dict] = None,
        runtime_environment: Optional[dict] = None,
    ) -> None:
        super().__init__(
            store_backend=store_backend,
            runtime_environment=runtime_environment,
            store_name=store_name,
        )

        # Gather the call arguments of the present function (include the "module_name" and add the "class_name"), filter
        # out the Falsy values, and set the instance "_config" variable equal to the resulting dictionary.
        self._config = {
            "store_backend": store_backend,
            "runtime_environment": runtime_environment,
            "store_name": store_name,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)

    def _validate_key(self, key: StringKey) -> None:
        from great_expectations.data_context.types.data_context_variables import (
            VariablesSchema,
        )

        attr: str = key.to_tuple()[0]
        if not VariablesSchema.has_value(attr):
            raise TypeError(
                f"{key} key must be an member of {VariablesSchema.__name__} (not {type(key)})"
            )
