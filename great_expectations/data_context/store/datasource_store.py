from typing import Optional, Union

from great_expectations.core.data_context_key import StringKey
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.types.base import (
    DatasourceConfig,
    DatasourceConfigSchema,
)
from great_expectations.util import filter_properties_dict


class DatasourceStore(Store):
    """
    A DatasourceStore manages Datasources for the DataContext.
    """

    _key_class = StringKey

    def __init__(
        self,
        store_name: Optional[str] = None,
        store_backend: Optional[dict] = None,
        runtime_environment: Optional[dict] = None,
    ) -> None:
        self._schema = DatasourceConfigSchema()
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

    def serialize(self, _, value: DatasourceConfig) -> Union[str, dict]:
        """
        See parent 'Store.serialize()' for more information
        """
        if self.ge_cloud_mode:
            # GeCloudStoreBackend expects a json str
            return self._schema.dump(value)
        return self._schema.dumps(value, indent=2, sort_keys=True)

    def deserialize(self, _, value: Union[str, dict]) -> DatasourceConfig:
        """
        See parent 'Store.deserialize()' for more information
        """
        if isinstance(value, dict):
            return self._schema.load(value)
        else:
            return self._schema.loads(value)
