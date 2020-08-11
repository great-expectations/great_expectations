from great_expectations.data_context.store.store import Store
from great_expectations.data_context.store.tuple_store_backend import TupleStoreBackend
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
)
from great_expectations.data_context.util import load_class
from great_expectations.util import (
    filter_properties_dict,
    get_currently_executing_function_call_arguments,
    verify_dynamic_loading_support,
)


class ConfigurationStore(Store):
    """
A Configuration Store provides a way to store GreatExpectations Configuration accessible to a Data Context.
    """

    _key_class = ConfigurationIdentifier

    def __init__(self, store_backend=None, runtime_environment=None):

        if store_backend is not None:
            store_backend_module_name = store_backend.get(
                "module_name", "great_expectations.data_context.store"
            )
            store_backend_class_name = store_backend.get(
                "class_name", "InMemoryStoreBackend"
            )
            verify_dynamic_loading_support(module_name=store_backend_module_name)
            store_backend_class = load_class(
                store_backend_class_name, store_backend_module_name
            )

            # Store Backend Class was loaded successfully; verify that it is of a correct subclass.
            if issubclass(store_backend_class, TupleStoreBackend):
                # Provide defaults for this common case
                store_backend["filepath_suffix"] = store_backend.get(
                    "filepath_suffix", ".yml"
                )
        super().__init__(
            store_backend=store_backend, runtime_environment=runtime_environment
        )

        self._config = get_currently_executing_function_call_arguments(
            include_module_name=True, **{"class_name": self.__class__.__name__,}
        )
        filter_properties_dict(properties=self._config, inplace=True)

    def remove_key(self, key):
        return self.store_backend.remove_key(key)

    @property
    def config(self):
        return self._config
