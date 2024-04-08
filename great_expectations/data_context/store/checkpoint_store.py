from __future__ import annotations

import json
import logging
import os
import uuid
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

from marshmallow import ValidationError

import great_expectations.exceptions as gx_exceptions
from great_expectations._docs_decorators import public_api
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.data_context_key import DataContextKey, StringKey
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.store import ConfigurationStore
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    DataContextConfigDefaults,
)
from great_expectations.data_context.types.refs import (
    GXCloudResourceRef,
)
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GXCloudIdentifier,
)

if TYPE_CHECKING:
    from great_expectations.checkpoint import Checkpoint
    from great_expectations.checkpoint.v1_checkpoint import Checkpoint as V1Checkpoint

logger = logging.getLogger(__name__)


class CheckpointStore(ConfigurationStore):
    """
    A CheckpointStore manages Checkpoints for the DataContext.
    """

    _configuration_class = CheckpointConfig

    @override
    @staticmethod
    def gx_cloud_response_json_to_object_dict(response_json: Dict) -> Dict:
        """
        This method takes full json response from GX cloud and outputs a dict appropriate for
        deserialization into a GX object
        """
        response_data = response_json["data"]

        cp_data: Dict
        if isinstance(response_data, list):
            if len(response_data) == 0:
                raise ValueError(f"Cannot parse empty data from GX Cloud payload: {response_json}")  # noqa: TRY003
            cp_data = response_data[0]
        else:
            cp_data = response_data

        ge_cloud_checkpoint_id: str = cp_data["id"]
        checkpoint_config_dict: Dict = cp_data["attributes"]["checkpoint_config"]
        checkpoint_config_dict["id"] = ge_cloud_checkpoint_id

        return checkpoint_config_dict

    @staticmethod
    def default_checkpoints_exist(directory_path: str) -> bool:
        if not directory_path:
            return False

        checkpoints_directory_path: str = os.path.join(  # noqa: PTH118
            directory_path,
            DataContextConfigDefaults.DEFAULT_CHECKPOINT_STORE_BASE_DIRECTORY_RELATIVE_NAME.value,
        )
        return os.path.isdir(checkpoints_directory_path)  # noqa: PTH112

    def list_checkpoints(
        self, ge_cloud_mode: bool = False
    ) -> Union[List[str], List[ConfigurationIdentifier]]:
        keys: Union[List[str], List[ConfigurationIdentifier]] = self.list_keys()  # type: ignore[assignment]
        if ge_cloud_mode:
            return keys
        return [k.configuration_key for k in keys]  # type: ignore[union-attr]

    def delete_checkpoint(
        self,
        name: str | None = None,
        id: str | None = None,
    ) -> None:
        key: Union[GXCloudIdentifier, ConfigurationIdentifier] = self.get_key(name=name, id=id)
        try:
            self.remove_key(key=key)
        except gx_exceptions.InvalidKeyError as exc_ik:
            raise gx_exceptions.CheckpointNotFoundError(
                message=f'Non-existent Checkpoint configuration named "{key.configuration_key}".\n\nDetails: {exc_ik}'  # type: ignore[union-attr]  # noqa: E501
            )

    def get_checkpoint(
        self, name: Optional[ConfigurationIdentifier | str], id: Optional[str]
    ) -> CheckpointConfig:
        key: GXCloudIdentifier | ConfigurationIdentifier
        if not isinstance(name, ConfigurationIdentifier):
            key = self.get_key(name=name, id=id)
        else:
            key = name

        try:
            checkpoint_config: Optional[Any] = self.get(key=key)
            assert isinstance(
                checkpoint_config, CheckpointConfig
            ), "checkpoint_config retrieved was not of type CheckpointConfig"
        except gx_exceptions.InvalidKeyError as exc_ik:
            raise gx_exceptions.CheckpointNotFoundError(
                message=f'Non-existent Checkpoint configuration named "{key.configuration_key}".\n\nDetails: {exc_ik}'  # type: ignore[union-attr]  # noqa: E501
            )
        except ValidationError as exc_ve:
            raise gx_exceptions.InvalidCheckpointConfigError(
                message="Invalid Checkpoint configuration", validation_error=exc_ve
            )

        return checkpoint_config

    def add_checkpoint(self, checkpoint: Checkpoint) -> Checkpoint | CheckpointConfig:
        """Persist a stand-alone Checkpoint object.

        Args:
            checkpoint: The Checkpoint to persist.

        Returns:
            The persisted Checkpoint (possibly modified state based on store backend).

        Raises:
            CheckpointError: If a Checkpoint with the given name already exists.
        """
        key = self._construct_key_from_checkpoint(checkpoint)
        try:
            return self._persist_checkpoint(key=key, checkpoint=checkpoint, persistence_fn=self.add)
        except gx_exceptions.StoreBackendError:
            raise gx_exceptions.CheckpointError(  # noqa: TRY003
                f"A Checkpoint named {checkpoint.name} already exists."
            )

    def update_checkpoint(self, checkpoint: Checkpoint) -> Checkpoint | CheckpointConfig:
        """Use a stand-alone Checkpoint object to update a persisted value.

        Args:
            checkpoint: The Checkpoint to use for updating.

        Returns:
            The persisted Checkpoint (possibly modified state based on store backend).

        Raises:
            CheckpointNotFoundError: If a Checkpoint with the given name does not exist in the store.
        """  # noqa: E501
        key = self._construct_key_from_checkpoint(checkpoint)
        try:
            return self._persist_checkpoint(
                key=key, checkpoint=checkpoint, persistence_fn=self.update
            )
        except gx_exceptions.StoreBackendError:
            raise gx_exceptions.CheckpointNotFoundError(  # noqa: TRY003
                f"Could not find an existing Checkpoint named {checkpoint.name}."
            )

    def add_or_update_checkpoint(self, checkpoint: Checkpoint) -> Checkpoint | CheckpointConfig:
        """Use a stand-alone Checkpoint object to either add or update a persisted value.

        Args:
            checkpoint: The Checkpoint to use for adding or updating.

        Returns:
            The persisted Checkpoint (possibly modified state based on store backend).
        """
        key = self._construct_key_from_checkpoint(checkpoint)
        return self._persist_checkpoint(
            key=key, checkpoint=checkpoint, persistence_fn=self.add_or_update
        )

    def _construct_key_from_checkpoint(
        self, checkpoint: Checkpoint
    ) -> GXCloudIdentifier | ConfigurationIdentifier:
        name = checkpoint.name
        id = checkpoint.id
        if id:
            return self.get_key(id=id)
        return self.get_key(name=name)

    def _persist_checkpoint(
        self,
        key: GXCloudIdentifier | ConfigurationIdentifier,
        checkpoint: Checkpoint,
        persistence_fn: Callable,
    ) -> Checkpoint | CheckpointConfig:
        checkpoint_ref = persistence_fn(key=key, value=checkpoint.get_config())
        if isinstance(checkpoint_ref, GXCloudResourceRef):
            # return CheckpointConfig from cloud POST response to account for any defaults/new ids added in cloud  # noqa: E501
            checkpoint_config = checkpoint_ref.response["data"]["attributes"]["checkpoint_config"]
            checkpoint_config["id"] = checkpoint_config.pop("id")
            return self.deserialize(checkpoint_config)
        elif self.cloud_mode:
            # if in cloud mode and checkpoint_ref is not a GXCloudResourceRef, a PUT operation occurred  # noqa: E501
            # re-fetch and return CheckpointConfig from cloud to account for any defaults/new ids added in cloud  # noqa: E501
            return self.get_checkpoint(name=checkpoint.name, id=None)

        return checkpoint

    @public_api
    def create(self, checkpoint_config: CheckpointConfig) -> Optional[DataContextKey]:
        """Create a checkpoint config in the store using a store_backend-specific key.

        Args:
            checkpoint_config: Config containing the checkpoint name.

        Returns:
            None unless using GXCloudStoreBackend and if so the GeCloudResourceRef which contains the id
            which was used to create the config in the backend.
        """  # noqa: E501
        # CheckpointConfig not an AbstractConfig??
        # mypy error: incompatible type "CheckpointConfig"; expected "AbstractConfig"
        key: DataContextKey = self._build_key_from_config(checkpoint_config)  # type: ignore[arg-type]

        # Make two separate requests to set and get in order to obtain any additional
        # values that may have been added to the config by the StoreBackend (i.e. object ids)
        ref: Optional[Union[bool, GXCloudResourceRef]] = self.set(key, checkpoint_config)
        if ref and isinstance(ref, GXCloudResourceRef):
            key.id = ref.id  # type: ignore[attr-defined]

        config = self.get(key=key)

        return config


# NOTE: Will eventually be promoted to 'CheckpointStore' once the legacy Checkpoint
#       and its related classes are removed
class V1CheckpointStore(Store):
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
