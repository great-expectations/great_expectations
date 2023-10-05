from __future__ import annotations

import logging
from pprint import pformat as pf
from typing import TYPE_CHECKING, Optional, Union

from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.data_context_key import (
    DataContextVariableKey,
)
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.types.base import (
    assetConfigSchema,
)
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

    def remove_key(self, key: Union[DataContextVariableKey, GXCloudIdentifier]) -> bool:
        """
        See parent `Store.remove_key()` for more information
        """
        return self._store_backend.remove_key(key.to_tuple())

    @override
    def serialize(self, value: FluentDataAsset) -> Union[str, dict]:
        """
        See parent 'Store.serialize()' for more information
        """
        return value._json_dict()

    @override
    def deserialize(self, value: dict) -> FluentDataAsset:
        """
        See parent 'Store.deserialize()' for more information
        """
        type_ = value.get("type")
        data_asset_model = _SourceFactories.type_lookup.get(type_)
        if not data_asset_model:
            raise LookupError(f"Unknown DataAsset 'type': '{type_}'")
        return data_asset_model(**value)

    @override
    @staticmethod
    def gx_cloud_response_json_to_object_dict(
        response_json: CloudResponsePayloadTD,  # type: ignore[override]
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
