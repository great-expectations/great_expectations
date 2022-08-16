from __future__ import annotations

import copy
from typing import List, Optional, Union

from great_expectations.core.data_context_key import DataContextVariableKey
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.store.store_backend import StoreBackend
from great_expectations.data_context.types.base import (
    DatasourceConfig,
    DatasourceConfigSchema,
)
from great_expectations.data_context.types.refs import GeCloudResourceRef
from great_expectations.data_context.types.resource_identifiers import GeCloudIdentifier
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
        keys_without_store_backend_id: List[str] = list(
            filter(
                lambda k: k != StoreBackend.STORE_BACKEND_ID_KEY,
                self._store_backend.list_keys(),
            )
        )
        return keys_without_store_backend_id

    def remove_key(self, key: Union[DataContextVariableKey, GeCloudIdentifier]) -> None:
        """
        See parent `Store.remove_key()` for more information
        """
        return self._store_backend.remove_key(key.to_tuple())

    def serialize(self, value: DatasourceConfig) -> Union[str, DatasourceConfig]:
        """
        See parent 'Store.serialize()' for more information
        """
        if self.ge_cloud_mode:
            # GeCloudStoreBackend expects a json str
            return self._schema.dump(value)
        return value

    def deserialize(self, value: Union[dict, DatasourceConfig]) -> DatasourceConfig:
        """
        See parent 'Store.deserialize()' for more information
        """
        # When using the InlineStoreBackend, objects are already converted to their respective config types.
        if isinstance(value, DatasourceConfig):
            return value
        elif isinstance(value, dict):
            return self._schema.load(value)
        else:
            return self._schema.loads(value)

    def ge_cloud_response_json_to_object_dict(self, response_json: dict) -> dict:
        """
        This method takes full json response from GE cloud and outputs a dict appropriate for
        deserialization into a GE object
        """
        datasource_ge_cloud_id: str = response_json["data"]["id_"]
        datasource_config_dict: dict = response_json["data"]["attributes"][
            "datasource_config"
        ]
        datasource_config_dict["ge_cloud_id"] = datasource_ge_cloud_id

        return datasource_config_dict

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
        datasource_key: Union[
            DataContextVariableKey, GeCloudIdentifier
        ] = self.store_backend.build_key(name=datasource_name)
        if not self.has_key(datasource_key):  # noqa: W601
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

    def delete(self, datasource_config: DatasourceConfig) -> None:
        """Deletes a DatasourceConfig persisted in the store using its config.

        Args:
            datasource_config: The config of the Datasource to delete.
        """

        self.remove_key(self._build_key_from_config(datasource_config))

    def _build_key_from_config(
        self, datasource_config: DatasourceConfig
    ) -> Union[GeCloudIdentifier, DataContextVariableKey]:
        if hasattr(datasource_config, "id_"):
            id_ = datasource_config.id_
        else:
            id_ = None
        if hasattr(datasource_config, "name"):
            name = datasource_config.name
        else:
            name = None
        return self.store_backend.build_key(
            name=name,
            id_=id_,
        )

    def set_by_name(
        self, datasource_name: str, datasource_config: DatasourceConfig
    ) -> None:
        """Persists a DatasourceConfig in the store by a given name.

        Args:
            datasource_name: The name of the Datasource to update.
            datasource_config: The config object to persist using the StoreBackend.
        """
        datasource_key: DataContextVariableKey = self._determine_datasource_key(
            datasource_name=datasource_name
        )
        self.set(datasource_key, datasource_config)

    def create(
        self, datasource_config: DatasourceConfig
    ) -> Optional[GeCloudResourceRef]:
        """Create a datasource config in the store using a store_backend-specific key.

        Args:
            datasource_config: Config containing the datasource name.

        Returns:
            None unless using GeCloudStoreBackend and if so the GeCloudResourceRef which contains the id
            which was used to create the config in the backend.
        """
        key: Union[
            GeCloudIdentifier, DataContextVariableKey
        ] = self._build_key_from_config(datasource_config)
        return self.set(key, datasource_config)

    def update_by_name(
        self, datasource_name: str, datasource_config: DatasourceConfig
    ) -> None:
        """Updates a DatasourceConfig that already exists in the store.

        Args:
            datasource_name: The name of the Datasource to retrieve.
            datasource_config: The config object to persist using the StoreBackend.

        Raises:
            ValueError if a DatasourceConfig is not found.
        """
        datasource_key: DataContextVariableKey = self._determine_datasource_key(
            datasource_name=datasource_name
        )
        if not self.has_key(datasource_key):  # noqa: W601
            raise ValueError(
                f"Unable to load datasource `{datasource_name}` -- no configuration found or invalid configuration."
            )

        self.set_by_name(
            datasource_name=datasource_name, datasource_config=datasource_config
        )

    def _determine_datasource_key(self, datasource_name: str) -> DataContextVariableKey:
        datasource_key = DataContextVariableKey(
            resource_name=datasource_name,
        )
        return datasource_key
