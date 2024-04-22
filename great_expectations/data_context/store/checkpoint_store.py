from __future__ import annotations

import json
import logging
import uuid
from typing import TYPE_CHECKING

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.data_context_key import DataContextKey, StringKey
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.types.resource_identifiers import (
    GXCloudIdentifier,
)

if TYPE_CHECKING:
    from great_expectations.checkpoint.v1_checkpoint import Checkpoint as V1Checkpoint

logger = logging.getLogger(__name__)


class CheckpointStore(Store):
    _key_class = StringKey

    def get_key(self, name: str, id: str | None = None) -> GXCloudIdentifier | StringKey:
        """Given a name and optional ID, build the correct key for use in the CheckpointStore."""
        if self.cloud_mode:
            return GXCloudIdentifier(
                resource_type=GXCloudRESTResource.CHECKPOINT,
                id=id,
                resource_name=name,
            )
        return self._key_class(key=name)

    @override
    @classmethod
    def gx_cloud_response_json_to_object_dict(cls, response_json: dict) -> dict:
        response_data = response_json["data"]

        checkpoint_data: dict
        if isinstance(response_data, list):
            if len(response_data) != 1:
                if len(response_data) == 0:
                    msg = f"Cannot parse empty data from GX Cloud payload: {response_json}"
                else:
                    msg = f"Cannot parse multiple items from GX Cloud payload: {response_json}"
                raise ValueError(msg)
            checkpoint_data = response_data[0]
        else:
            checkpoint_data = response_data

        return cls._convert_raw_json_to_object_dict(checkpoint_data)

    @override
    @staticmethod
    def _convert_raw_json_to_object_dict(data: dict) -> dict:
        id: str = data["id"]
        checkpoint_config_dict: dict = data["attributes"]["checkpoint_config"]
        checkpoint_config_dict["id"] = id

        return checkpoint_config_dict

    @override
    def serialize(self, value):
        # In order to enable the custom json_encoders in Checkpoint, we need to set `models_as_dict` off  # noqa: E501
        # Ref: https://docs.pydantic.dev/1.10/usage/exporting_models/#serialising-self-reference-or-other-models
        data = value.json(models_as_dict=False, indent=2, sort_keys=True)
        if self.cloud_mode:
            return json.loads(data)

        return data

    @override
    def deserialize(self, value):
        from great_expectations.checkpoint.v1_checkpoint import Checkpoint as V1Checkpoint

        if self.cloud_mode:
            return V1Checkpoint.parse_obj(value)

        return V1Checkpoint.parse_raw(value)

    @override
    def _add(self, key: DataContextKey, value: V1Checkpoint, **kwargs):
        if not self.cloud_mode:
            value.id = str(uuid.uuid4())
        return super()._add(key=key, value=value, **kwargs)

    @override
    def _update(self, key: DataContextKey, value: V1Checkpoint, **kwargs):
        try:
            super()._update(key=key, value=value, **kwargs)
        except gx_exceptions.StoreBackendError as e:
            name = key.to_tuple()[0]
            raise ValueError(f"Could not update Checkpoint '{name}'") from e  # noqa: TRY003
