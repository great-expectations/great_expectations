import random
import uuid
from typing import Union

from great_expectations.data_context.store.configuration_store import ConfigurationStore
from great_expectations.data_context.store.database_store_backend import (
    DatabaseStoreBackend,
)
from great_expectations.data_context.store.tuple_store_backend import TupleStoreBackend
from great_expectations.data_context.types.base import (
    DatasourceConfig,
    DatasourceConfigSchema,
)
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    DatasourceIdentifier,
    GeCloudIdentifier,
)
from great_expectations.util import (
    filter_properties_dict,
    load_class,
    verify_dynamic_loading_support,
)


class DatasourceStore(Store):
    """
    A DatasourceStore manages Datasources for the DataContext.
    """

    _key_class = DatasourceIdentifier

    def __init__(
        self,
        store_backend=None,
        runtime_environment=None,
        store_name=None,
        data_context=None,
    ) -> None:
        self._schema = DatasourceConfigSchema()
        self._data_context = data_context
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
                    "filepath_suffix", ".json"
                )
            elif issubclass(store_backend_class, DatabaseStoreBackend):
                # Provide defaults for this common case
                store_backend["table_name"] = store_backend.get(
                    "table_name", "ge_expectations_store"
                )
                store_backend["key_columns"] = store_backend.get(
                    "key_columns", ["expectation_suite_name"]
                )

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

    # def ge_cloud_response_json_to_object_dict(self, response_json: Dict) -> Dict:
    #     """
    #     This method takes full json response from GE cloud and outputs a dict appropriate for
    #     deserialization into a GE object
    #     """
    #     ge_cloud_expectation_suite_id = response_json["data"]["id"]
    #     expectation_suite_dict = response_json["data"]["attributes"]["suite"]
    #     expectation_suite_dict["ge_cloud_id"] = ge_cloud_expectation_suite_id

    #     return expectation_suite_dict

    def get(self, key) -> DatasourceConfig:
        return super().get(key)

    def remove_key(self, key):
        return self.store_backend.remove_key(key)

    def serialize(self, key, value):
        if self.ge_cloud_mode:
            # GeCloudStoreBackend expects a json str
            return self._schema.dump(value)
        return self._schema.dumps(value, indent=2, sort_keys=True)

    def deserialize(self, key, value):
        if isinstance(value, dict):
            return self._schema.load(value)
        else:
            return self._schema.loads(value)
