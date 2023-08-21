from __future__ import annotations

import logging
from pprint import pformat as pf
from typing import TYPE_CHECKING, Optional, Union, overload

from great_expectations.core.data_context_key import (
    DataContextKey,
    DataContextVariableKey,
)
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.types.base import (
    AssetConfig,
    assetConfigSchema,
)
from great_expectations.data_context.types.refs import GXCloudResourceRef
from great_expectations.datasource.fluent.sources import _SourceFactories
from great_expectations.util import filter_properties_dict

if TYPE_CHECKING:
    from typing import Literal

    from typing_extensions import TypedDict

    from great_expectations.core.serializer import AbstractConfigSerializer
    from great_expectations.data_context.types.resource_identifiers import (
        GXCloudIdentifier,
    )
    from great_expectations.datasource.fluent import (
        DataAsset as FluentDataAsset,
    )

    class DataPayload(TypedDict):
        id: str
        attributes: dict
        type: Literal["data_asset"]

    class CloudResponsePayloadTD(TypedDict):
        data: DataPayload | list[DataPayload]


logger = logging.getLogger(__name__)


class DataAssetStore(Store):
    """
    A DataAssetStore manages DataAssets for CloudDataContexts.
    """

    _key_class = DataContextVariableKey

    def __init__(
        self,
        serializer: AbstractConfigSerializer,
        store_name: Optional[str] = None,
        store_backend: Optional[dict] = None,
        runtime_environment: Optional[dict] = None,
    ) -> None:
        self._schema = assetConfigSchema
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
        self, value: AssetConfig | FluentDataAsset
    ) -> Union[str, dict, AssetConfig]:
        """
        See parent 'Store.serialize()' for more information
        """
        return self._serializer.serialize(value)

    @overload
    def deserialize(self, value: AssetConfig) -> AssetConfig:
        ...

    @overload
    def deserialize(self, value: FluentDataAsset) -> FluentDataAsset:
        ...

    def deserialize(self, value: Union[dict, str]) -> AssetConfig | FluentDataAsset:
        """
        See parent 'Store.deserialize()' for more information
        """
        if isinstance(value, dict):
            # presence of a 'type' field means it's a fluent DataAsset
            type_ = value.get("type")
            if type_:
                data_asset_model = _SourceFactories.type_lookup.get(type_)
                if not data_asset_model:
                    raise LookupError(f"Unknown DataAsset 'type': '{type_}'")
                return data_asset_model(**value)
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
                # TODO: handle larger arrays of DataAssets
                raise TypeError(
                    f"GX Cloud returned {len(data)} DataAssets but expected 1"
                )
            data = data[0]
        data_asset_ge_cloud_id: str = data["id"]
        data_asset_config_dict: dict = data["attributes"]["data_asset_config"]
        data_asset_config_dict["id"] = data_asset_ge_cloud_id

        return data_asset_config_dict

    def delete(self, data_asset_config: AssetConfig | FluentDataAsset) -> None:
        """Deletes an AssetConfig persisted in the store using its config.

        Args:
            data_asset_config: The config of the DataAsset to delete.
        """

        self.remove_key(self._build_key_from_config(data_asset_config))

    def _build_key_from_config(  # type: ignore[override]
        self, data_asset_config: AssetConfig | FluentDataAsset
    ) -> Union[GXCloudIdentifier, DataContextVariableKey]:
        id_: str | None = (
            str(data_asset_config.id)
            if data_asset_config.id
            else data_asset_config.id  # type: ignore[assignment] #
            # uuid
            # will be converted to str
        )
        return self.store_backend.build_key(name=data_asset_config.name, id=id_)

    @overload  # type: ignore[override]
    def set(
        self,
        key: Union[DataContextKey, None],
        value: FluentDataAsset,
        **kwargs,
    ) -> FluentDataAsset:
        ...

    @overload
    def set(
        self,
        key: Union[DataContextKey, None],
        value: AssetConfig,
        **kwargs,
    ) -> AssetConfig:
        ...

    def set(
        self,
        key: Union[DataContextKey, None],
        value: AssetConfig | FluentDataAsset,
        **kwargs,
    ) -> AssetConfig | FluentDataAsset:
        """Create a DataAsset config in the store using a store_backend-specific key.
        Args:
            key: Optional key to use when setting value.
            value: AssetConfig set in the store at the key provided or created from the AssetConfig attributes.
            **_: kwargs will be ignored but accepted to align with the parent class.
        Returns:
            AssetConfig retrieved from the DataAssetStore.
        """
        if not key:
            key = self._build_key_from_config(value)
        return self._persist_data_asset(key=key, config=value)

    def _persist_data_asset(
        self, key: DataContextKey, config: AssetConfig | FluentDataAsset
    ) -> AssetConfig:
        # Make two separate requests to set and get in order to obtain any additional
        # values that may have been added to the config by the StoreBackend (i.e. object ids)
        ref: Optional[Union[bool, GXCloudResourceRef]] = super().set(
            key=key, value=config
        )
        if ref and isinstance(ref, GXCloudResourceRef):
            key.id = ref.id  # type: ignore[attr-defined]

        return_value: AssetConfig = self.get(key)  # type: ignore[assignment]
        if not return_value.name and isinstance(key, DataContextVariableKey):
            # Setting the name in the config is currently needed to handle adding the name to v2 data_asset
            # configs and can be refactored (e.g. into `get()`)
            return_value.name = key.resource_name

        return return_value
