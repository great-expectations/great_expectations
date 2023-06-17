from __future__ import annotations

import copy
import logging
from pprint import pformat as pf
from typing import TYPE_CHECKING, Optional, Union, overload

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.data_context_key import (
    DataContextKey,
    DataContextVariableKey,
)
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.types.base import (
    DatasourceConfig,
    datasourceConfigSchema,
)
from great_expectations.data_context.types.refs import GXCloudResourceRef
from great_expectations.datasource.fluent import Datasource as FluentDatasource
from great_expectations.datasource.fluent.sources import _SourceFactories
from great_expectations.util import filter_properties_dict

if TYPE_CHECKING:
    from typing import Literal

    from typing_extensions import TypedDict

    from great_expectations.core.serializer import AbstractConfigSerializer
    from great_expectations.data_context.types.resource_identifiers import (
        GXCloudIdentifier,
    )

    class DataPayload(TypedDict):
        id: str
        attributes: dict
        type: Literal["datasource"]

    class CloudResponsePayloadTD(TypedDict):
        data: DataPayload | list[DataPayload]


logger = logging.getLogger(__name__)


class DatasourceStore(Store):
    """
    A DatasourceStore manages Datasources for the DataContext.
    """

    _key_class = DataContextVariableKey

    def __init__(
        self,
        serializer: AbstractConfigSerializer,
        store_name: Optional[str] = None,
        store_backend: Optional[dict] = None,
        runtime_environment: Optional[dict] = None,
    ) -> None:
        self._schema = datasourceConfigSchema
        self._serializer = serializer
        super().__init__(
            store_backend=store_backend,
            runtime_environment=runtime_environment,
            store_name=store_name,  # type: ignore[arg-type]
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

    def remove_key(self, key: Union[DataContextVariableKey, GXCloudIdentifier]) -> None:
        """
        See parent `Store.remove_key()` for more information
        """
        return self._store_backend.remove_key(key.to_tuple())

    def serialize(
        self, value: DatasourceConfig | FluentDatasource
    ) -> Union[str, dict, DatasourceConfig]:
        """
        See parent 'Store.serialize()' for more information
        """
        if isinstance(value, FluentDatasource):
            return value._json_dict()
        return self._serializer.serialize(value)

    @overload
    def deserialize(self, value: DatasourceConfig) -> DatasourceConfig:
        ...

    @overload
    def deserialize(self, value: FluentDatasource) -> FluentDatasource:
        ...

    def deserialize(
        self, value: dict | DatasourceConfig | FluentDatasource
    ) -> DatasourceConfig | FluentDatasource:
        """
        See parent 'Store.deserialize()' for more information
        """
        # When using the InlineStoreBackend, objects are already converted to their respective config types.
        if isinstance(value, (DatasourceConfig, FluentDatasource)):
            return value
        elif isinstance(value, dict):
            # presence of a 'type' field means it's a fluent datasource
            type_ = value.get("type")
            if type_:
                datasource_model = _SourceFactories.type_lookup.get(type_)
                if not datasource_model:
                    raise LookupError(f"Unknown Datasource 'type': '{type_}'")
                return datasource_model(**value)
            return self._schema.load(value)
        else:
            return self._schema.loads(value)

    def ge_cloud_response_json_to_object_dict(
        self, response_json: CloudResponsePayloadTD  # type: ignore[override]
    ) -> dict:
        """
        This method takes full json response from GX cloud and outputs a dict appropriate for
        deserialization into a GX object
        """
        logger.debug(f"GE Cloud Response JSON ->\n{pf(response_json, depth=3)}")
        data = response_json["data"]
        if isinstance(data, list):
            if len(data) > 1:
                # TODO: handle larger arrays of Datasources
                raise TypeError(
                    f"GX Cloud returned {len(data)} Datasources but expected 1"
                )
            data = data[0]
        datasource_ge_cloud_id: str = data["id"]
        datasource_config_dict: dict = data["attributes"]["datasource_config"]
        datasource_config_dict["id"] = datasource_ge_cloud_id

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
            DataContextVariableKey, GXCloudIdentifier
        ] = self.store_backend.build_key(name=datasource_name)
        if not self.has_key(datasource_key):
            raise ValueError(
                f"Unable to load datasource `{datasource_name}` -- no configuration found or invalid configuration."
            )

        datasource_config: DatasourceConfig = copy.deepcopy(self.get(datasource_key))  # type: ignore[assignment]
        return datasource_config

    def delete(self, datasource_config: DatasourceConfig | FluentDatasource) -> None:
        """Deletes a DatasourceConfig persisted in the store using its config.

        Args:
            datasource_config: The config of the Datasource to delete.
        """

        self.remove_key(self._build_key_from_config(datasource_config))

    def _build_key_from_config(  # type: ignore[override]
        self, datasource_config: DatasourceConfig | FluentDatasource
    ) -> Union[GXCloudIdentifier, DataContextVariableKey]:
        id_: str | None = (
            str(datasource_config.id) if datasource_config.id else datasource_config.id  # type: ignore[assignment] # uuid will be converted to str
        )
        return self.store_backend.build_key(name=datasource_config.name, id=id_)

    @overload  # type: ignore[override]
    def set(
        self,
        key: Union[DataContextKey, None],
        value: FluentDatasource,
        **kwargs,
    ) -> FluentDatasource:
        ...

    @overload
    def set(
        self,
        key: Union[DataContextKey, None],
        value: DatasourceConfig,
        **kwargs,
    ) -> DatasourceConfig:
        ...

    def set(
        self,
        key: Union[DataContextKey, None],
        value: DatasourceConfig | FluentDatasource,
        **kwargs,
    ) -> DatasourceConfig | FluentDatasource:
        """Create a datasource config in the store using a store_backend-specific key.
        Args:
            key: Optional key to use when setting value.
            value: DatasourceConfig set in the store at the key provided or created from the DatasourceConfig attributes.
            **_: kwargs will be ignored but accepted to align with the parent class.
        Returns:
            DatasourceConfig retrieved from the DatasourceStore.
        """
        if not key:
            key = self._build_key_from_config(value)
        return self._persist_datasource(key=key, config=value)

    def _persist_datasource(
        self, key: DataContextKey, config: DatasourceConfig | FluentDatasource
    ) -> DatasourceConfig:
        # Make two separate requests to set and get in order to obtain any additional
        # values that may have been added to the config by the StoreBackend (i.e. object ids)
        ref: Optional[Union[bool, GXCloudResourceRef]] = super().set(
            key=key, value=config
        )
        if ref and isinstance(ref, GXCloudResourceRef):
            key.id = ref.id  # type: ignore[attr-defined]

        return_value: DatasourceConfig = self.get(key)  # type: ignore[assignment]
        if not return_value.name and isinstance(key, DataContextVariableKey):
            # Setting the name in the config is currently needed to handle adding the name to v2 datasource
            # configs and can be refactored (e.g. into `get()`)
            return_value.name = key.resource_name

        return return_value

    def add_by_name(
        self, datasource_name: str, datasource_config: DatasourceConfig
    ) -> None:
        """Persists a DatasourceConfig in the store by a given name.

        Args:
            datasource_name: The name of the Datasource to update.
            datasource_config: The config object to persist using the StoreBackend.

        Raises:
            DatasourceError: A DatasourceConfig with the given key already exists in the store.
        """
        datasource_key: DataContextVariableKey = self._determine_datasource_key(
            datasource_name=datasource_name
        )
        try:
            self.add(key=datasource_key, value=datasource_config)
        except gx_exceptions.StoreBackendError:
            raise gx_exceptions.DatasourceError(
                datasource_name=datasource_name,
                message="A Datasource with the given name already exists",
            )

    def update_by_name(
        self, datasource_name: str, datasource_config: DatasourceConfig
    ) -> None:
        """Updates a DatasourceConfig that already exists in the store.

        Args:
            datasource_name: The name of the Datasource to retrieve.
            datasource_config: The config object to persist using the StoreBackend.

        Raises:
            DatasourceNotFoundError: If a DatasourceConfig is not found.
        """
        datasource_key: DataContextVariableKey = self._determine_datasource_key(
            datasource_name=datasource_name
        )
        try:
            self.update(key=datasource_key, value=datasource_config)
        except gx_exceptions.StoreBackendError:
            raise gx_exceptions.DatasourceNotFoundError(
                f"Could not find an existing Datasource named {datasource_name}."
            )

    def _determine_datasource_key(self, datasource_name: str) -> DataContextVariableKey:
        datasource_key = DataContextVariableKey(
            resource_name=datasource_name,
        )
        return datasource_key
