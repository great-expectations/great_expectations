import copy
from typing import Any, List, Optional, Tuple, Union

from great_expectations.core.data_context_key import DataContextVariableKey
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.types.base import (
    DatasourceConfig,
    DatasourceConfigSchema,
)
from great_expectations.data_context.types.data_context_variables import (
    DataContextVariableSchema,
)
from great_expectations.util import filter_properties_dict


class DatasourceStore(Store):
    """
    A DatasourceStore manages Datasources for the DataContext.
    """

    _key_class = DataContextVariableKey

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

    def list_keys(self) -> List[str]:
        """
        See parent 'Store.list_keys()' for more information
        """
        from great_expectations.data_context.types.data_context_variables import (
            DataContextVariableSchema,
        )

        datasource_key: Tuple[DataContextVariableSchema] = (
            DataContextVariableSchema.DATASOURCES,
        )

        keys_without_store_backend_id: List[str] = [
            key for key in self._store_backend.list_keys(prefix=datasource_key)
        ]
        return [key for key in keys_without_store_backend_id]

    def remove_key(self, key: DataContextVariableKey) -> None:
        """
        See parent `Store.remove_key()` for more information
        """
        return self._store_backend.remove_key(key.to_tuple())

    def serialize(
        self, key: Optional[Any], value: DatasourceConfig
    ) -> Union[str, DatasourceConfig]:
        """
        See parent 'Store.serialize()' for more information
        """
        del key  # Unused arg but necessary as part of signature
        if self.ge_cloud_mode:
            # GeCloudStoreBackend expects a json str
            return self._schema.dump(value)
        return value

    def deserialize(
        self, key: Optional[Any], value: Union[dict, DatasourceConfig]
    ) -> DatasourceConfig:
        """
        See parent 'Store.deserialize()' for more information
        """
        del key  # Unused arg but necessary as part of signature

        # When using the InlineStoreBackend, objects are already converted to their respective config types.
        if isinstance(value, DatasourceConfig):
            return value
        elif isinstance(value, dict):
            return self._schema.load(value)
        else:
            return self._schema.loads(value)

    def retrieve_by_name(self, datasource_name: str) -> DatasourceConfig:
        """Retrieves a DatasourceConfig persisted in the store by it's given name.

        Args:
            datasource_name: The name of the Datasource to retrieve.

        Returns:
            The DatasourceConfig persisted in the store that is associated with the given
            input datasource_name.

        Raises:
            ValueError if a DatasourceConfig is not found.
        """
        datasource_key: DataContextVariableKey = self._determine_datasource_key(
            datasource_name=datasource_name
        )
        if not self.has_key(datasource_key):
            raise ValueError(
                f"Unable to load datasource `{datasource_name}` -- no configuration found or invalid configuration."
            )

        datasource_config: DatasourceConfig = copy.deepcopy(self.get(datasource_key))
        return datasource_config

    def delete_by_name(self, datasource_name: str) -> None:
        """Deletes a DatasourceConfig persisted in the store by it's given name.

        Args:
            datasource_name: The name of the Datasource to retrieve.
        """
        datasource_key: DataContextVariableKey = self._determine_datasource_key(
            datasource_name=datasource_name
        )
        self.remove_key(datasource_key)

    def set_by_name(
        self, datasource_name: str, datasource_config: DatasourceConfig
    ) -> None:
        datasource_key: DataContextVariableKey = self._determine_datasource_key(
            datasource_name=datasource_name
        )
        self.set(datasource_key, datasource_config)

    def _determine_datasource_key(self, datasource_name: str) -> DataContextVariableKey:
        datasource_key: DataContextVariableKey = DataContextVariableKey(
            resource_type=DataContextVariableSchema.DATASOURCES,
            resource_name=datasource_name,
        )
        return datasource_key
