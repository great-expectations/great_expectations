from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING

from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.data_context_key import DataContextKey, StringKey
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.store.tuple_store_backend import TupleStoreBackend
from great_expectations.data_context.types.resource_identifiers import (
    GXCloudIdentifier,
)

if TYPE_CHECKING:
    from great_expectations.core.validation_definition import ValidationDefinition


class ValidationDefinitionStore(Store):
    _key_class = StringKey

    def __init__(
        self,
        store_backend: dict | None = None,
        runtime_environment: dict | None = None,
        store_name: str = "no_store_name",
    ) -> None:
        store_backend_class = self._determine_store_backend_class(store_backend)
        if store_backend and issubclass(store_backend_class, TupleStoreBackend):
            store_backend["filepath_suffix"] = store_backend.get("filepath_suffix", ".json")

        super().__init__(
            store_backend=store_backend,
            runtime_environment=runtime_environment,
            store_name=store_name,
        )

    def get_key(self, name: str, id: str | None = None) -> GXCloudIdentifier | StringKey:
        """Given a name and optional ID, build the correct key for use in the ValidationDefinitionStore."""  # noqa: E501
        if self.cloud_mode:
            return GXCloudIdentifier(
                resource_type=GXCloudRESTResource.VALIDATION_DEFINITION,
                id=id,
                resource_name=name,
            )
        return StringKey(key=name)

    @override
    @staticmethod
    def gx_cloud_response_json_to_object_dict(response_json: dict) -> dict:
        response_data = response_json["data"]

        validation_data: dict
        if isinstance(response_data, list):
            if len(response_data) != 1:
                if len(response_data) == 0:
                    msg = f"Cannot parse empty data from GX Cloud payload: {response_json}"
                else:
                    msg = f"Cannot parse multiple items from GX Cloud payload: {response_json}"
                raise ValueError(msg)
            validation_data = response_data[0]
        else:
            validation_data = response_data

        return validation_data

    @override
    @staticmethod
    def _convert_raw_json_to_object_dict(data: dict) -> dict:
        return data

    @override
    def serialize(self, value):
        # In order to enable the custom json_encoders in ValidationDefinition, we need to set `models_as_dict` off  # noqa: E501
        # Ref: https://docs.pydantic.dev/1.10/usage/exporting_models/#serialising-self-reference-or-other-models
        output = value.json(models_as_dict=False, indent=2, sort_keys=True)

        if self.cloud_mode:
            output_dict = json.loads(output)
            output_dict.pop("id", None)
            return output_dict
        else:
            return output

    @override
    def deserialize(self, value):
        from great_expectations.core.validation_definition import ValidationDefinition

        if self.cloud_mode:
            return ValidationDefinition.parse_obj(value)

        return ValidationDefinition.parse_raw(value)

    @override
    def _add(self, key: DataContextKey, value: ValidationDefinition, **kwargs):
        if not self.cloud_mode:
            # this logic should move to the store backend, but is implemented here for now
            value.id = str(uuid.uuid4())
        return super()._add(key=key, value=value, **kwargs)
