from __future__ import annotations

from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.data_context_key import StringKey
from great_expectations.core.validation_config import ValidationConfig
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.types.resource_identifiers import (
    GXCloudIdentifier,
)


class ValidationConfigStore(Store):
    _key_class = StringKey

    def get_key(
        self, name: str, id: str | None = None
    ) -> GXCloudIdentifier | StringKey:
        """Given a name and optional ID, build the correct key for use in the ValidationConfigStore."""
        if self.cloud_mode:
            return GXCloudIdentifier(
                resource_type=GXCloudRESTResource.VALIDATION_CONFIG,
                id=id,
                resource_name=name,
            )
        return StringKey(key=name)

    @override
    def serialize(self, value):
        if self.cloud_mode:
            data = value.dict()
            data["suite"] = data["suite"].to_json_dict()
            return data

        # In order to enable the custom json_encoders in ValidationConfig, we need to set `models_as_dict` off
        # Ref: https://docs.pydantic.dev/1.10/usage/exporting_models/#serialising-self-reference-or-other-models
        return value.json(models_as_dict=False, indent=2, sort_keys=True)

    @override
    def deserialize(self, value):
        return ValidationConfig.parse_raw(value)
