from __future__ import annotations

import copy
import logging
import warnings
from pprint import pformat as pf
from typing import TYPE_CHECKING, Optional, Union

from great_expectations.compatibility.pydantic import (
    ValidationError as PydanticValidationError,
)
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.data_context_key import (
    DataContextKey,
    DataContextVariableKey,
)
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.types.refs import GXCloudResourceRef
from great_expectations.datasource.fluent import Datasource as FluentDatasource
from great_expectations.datasource.fluent import (
    GxInvalidDatasourceWarning,
    InvalidDatasource,
)
from great_expectations.datasource.fluent.sources import DataSourceManager
from great_expectations.util import filter_properties_dict

if TYPE_CHECKING:
    from typing_extensions import TypedDict

    from great_expectations.data_context.types.resource_identifiers import (
        GXCloudIdentifier,
    )
    from great_expectations.datasource.fluent.fluent_base_model import MappingIntStrAny

    class DataPayload(TypedDict):
        id: str
        type: str
        name: str

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
        store_name: Optional[str] = None,
        store_backend: Optional[dict] = None,
        runtime_environment: Optional[dict] = None,
    ) -> None:
        super().__init__(
            store_backend=store_backend,
            runtime_environment=runtime_environment,
            store_name=store_name,  # type: ignore[arg-type]
        )

        # Gather the call arguments of the present function (include the "module_name" and add the "class_name"), filter  # noqa: E501
        # out the Falsy values, and set the instance "_config" variable equal to the resulting dictionary.  # noqa: E501
        self._config = {
            "store_backend": store_backend,
            "runtime_environment": runtime_environment,
            "store_name": store_name,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)

    @override
    def remove_key(self, key: Union[DataContextVariableKey, GXCloudIdentifier]) -> None:
        """
        See parent `Store.remove_key()` for more information
        """
        return self._store_backend.remove_key(key.to_tuple())

    @override
    def serialize(self, value: FluentDatasource) -> dict:
        """
        See parent 'Store.serialize()' for more information
        """
        # DataAsset.order_by is not supported by v1, but has not yet been removed,
        # so we drop it before serialization and alert the user with a warning.
        if any(asset.order_by for asset in value.assets):
            asset_names = [asset.name for asset in value.assets if asset.order_by]

            warnings.warn(
                f"Datasource {value.name} has one or more DataAssets that define a non-empty "
                "order_by field. This property is no longer supported, and will be "
                "silently dropped during serialization. The following DataAsset(s) are affected: "
                + ", ".join(asset_names),
                category=UserWarning,
            )
        exclude: MappingIntStrAny = {"assets": {"__all__": {"order_by"}}}
        return value._json_dict(exclude=exclude)

    @override
    def deserialize(self, value: dict | FluentDatasource) -> FluentDatasource:
        """
        See parent 'Store.deserialize()' for more information
        """
        # When using the InlineStoreBackend, objects are already converted to their respective config types.  # noqa: E501
        if isinstance(value, FluentDatasource):
            return value
        else:
            type_: str | None = value["type"]
            if not type_:
                raise ValueError("Datasource type is missing")  # noqa: TRY003
            try:
                datasource_model = DataSourceManager.type_lookup[type_]
                return datasource_model(**value)
            except (PydanticValidationError, LookupError) as config_error:
                warnings.warn(
                    f"Datasource {value.get('name', '')} configuration is invalid."
                    " Check `my_datasource.config_error` attribute for more details.",
                    GxInvalidDatasourceWarning,
                )
                # Any fields that are not part of the schema are ignored
                return InvalidDatasource(config_error=config_error, **value)

    @override
    @classmethod
    def gx_cloud_response_json_to_object_dict(
        cls,
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
                # Larger arrays of datasources should be handled by `gx_cloud_response_json_to_object_collection`  # noqa: E501
                raise TypeError(f"GX Cloud returned {len(data)} Datasources but expected 1")  # noqa: TRY003
            data = data[0]

        return DatasourceStore._convert_raw_json_to_object_dict(data)

    @override
    @staticmethod
    def _convert_raw_json_to_object_dict(data: DataPayload) -> dict:  # type: ignore[override]
        return data  # type: ignore[return-value]

    def retrieve_by_name(self, name: str) -> FluentDatasource:
        """Retrieves a Datasource persisted in the store by it's given name.

        Args:
            name: The name of the Datasource to retrieve.

        Returns:
            The Datasource persisted in the store that is associated with the given
            input name.

        Raises:
            ValueError if a Datasource is not found.
        """
        datasource_key: Union[DataContextVariableKey, GXCloudIdentifier] = (
            self.store_backend.build_key(name=name)
        )
        if not self.has_key(datasource_key):
            raise ValueError(  # noqa: TRY003
                f"Unable to load datasource `{name}` -- no configuration found or invalid configuration."  # noqa: E501
            )

        datasource_config: FluentDatasource = copy.deepcopy(self.get(datasource_key))  # type: ignore[arg-type]
        datasource_config.name = name
        return datasource_config

    def delete(self, datasource_config: FluentDatasource) -> None:
        """Deletes a Datasource persisted in the store using its config.

        Args:
            datasource_config: The config of the Datasource to delete.
        """

        self.remove_key(self._build_key_from_config(datasource_config))

    @override
    def _build_key_from_config(  # type: ignore[override]
        self, datasource_config: FluentDatasource
    ) -> Union[GXCloudIdentifier, DataContextVariableKey]:
        id_: str | None = (
            str(datasource_config.id) if datasource_config.id else datasource_config.id  # type: ignore[assignment] # uuid will be converted to str
        )
        return self.store_backend.build_key(name=datasource_config.name, id=id_)

    def get_fluent_datasource_by_name(self, name: str) -> FluentDatasource:
        # TODO: Delete this when we remove block style datasource configs
        key = DataContextVariableKey(
            resource_name=name,
        )
        datasource = self.get(key)
        if not isinstance(datasource, FluentDatasource):
            raise ValueError("Datasource is not a FluentDatasource")  # noqa: TRY003, TRY004
        return datasource

    @override
    def set(
        self,
        key: Union[DataContextKey, None],
        value: FluentDatasource,
        **kwargs,
    ) -> FluentDatasource:
        """Create a datasource config in the store using a store_backend-specific key.
        Args:
            key: Optional key to use when setting value.
            value: Datasource set in the store at the key provided or created from the Datsource.
            **_: kwargs will be ignored but accepted to align with the parent class.
        Returns:
            Datasource retrieved from the DatasourceStore.
        """
        if not key:
            key = self._build_key_from_config(value)
        return self._persist_datasource(key=key, config=value)

    def _persist_datasource(
        self, key: DataContextKey, config: FluentDatasource
    ) -> FluentDatasource:
        # Make two separate requests to set and get in order to obtain any additional
        # values that may have been added to the config by the StoreBackend (i.e. object ids)
        ref: Optional[Union[bool, GXCloudResourceRef]] = super().set(key=key, value=config)
        if ref and isinstance(ref, GXCloudResourceRef):
            key.id = ref.id  # type: ignore[attr-defined]

        return_value: FluentDatasource = self.get(key)  # type: ignore[assignment]
        if not return_value.name and isinstance(key, DataContextVariableKey):
            # Setting the name in the config is currently needed to handle adding the name to v2 datasource  # noqa: E501
            # configs and can be refactored (e.g. into `get()`)
            if not key.resource_name:
                raise ValueError("Missing resource name")  # noqa: TRY003
            return_value.name = key.resource_name

        return return_value

    def _determine_datasource_key(self, name: str) -> DataContextVariableKey:
        datasource_key = DataContextVariableKey(
            resource_name=name,
        )
        return datasource_key
