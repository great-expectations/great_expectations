from __future__ import annotations

import logging
import os
import random
import uuid
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

from marshmallow import ValidationError

import great_expectations.exceptions as gx_exceptions
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.data_context_key import DataContextKey  # noqa: TCH001
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.store import ConfigurationStore
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

logger = logging.getLogger(__name__)


class CheckpointStore(ConfigurationStore):
    """
    A CheckpointStore manages Checkpoints for the DataContext.
    """

    _configuration_class = CheckpointConfig

    def ge_cloud_response_json_to_object_dict(self, response_json: Dict) -> Dict:
        """
        This method takes full json response from GX cloud and outputs a dict appropriate for
        deserialization into a GX object
        """
        cp_data: Dict
        if isinstance(response_json["data"], list):
            cp_data = response_json["data"][0]
        else:
            cp_data = response_json["data"]
        ge_cloud_checkpoint_id: str = cp_data["id"]
        checkpoint_config_dict: Dict = cp_data["attributes"]["checkpoint_config"]
        checkpoint_config_dict["ge_cloud_id"] = ge_cloud_checkpoint_id

        # Checkpoints accept a `ge_cloud_id` but not an `id`
        checkpoint_config_dict.pop("id", None)

        return checkpoint_config_dict

    def serialization_self_check(self, pretty_print: bool) -> None:
        test_checkpoint_name: str = "test-name-" + "".join(
            [random.choice(list("0123456789ABCDEF")) for i in range(20)]
        )
        test_checkpoint_configuration = CheckpointConfig(
            **{"name": test_checkpoint_name}  # type: ignore[arg-type]
        )
        if self.cloud_mode:
            test_key: GXCloudIdentifier = self.key_class(  # type: ignore[call-arg,assignment]
                resource_type=GXCloudRESTResource.CHECKPOINT,
                ge_cloud_id=str(uuid.uuid4()),
            )
        else:
            test_key = self.key_class(configuration_key=test_checkpoint_name)  # type: ignore[call-arg,assignment]

        if pretty_print:
            print(f"Attempting to add a new test key {test_key} to Checkpoint store...")
        self.set(key=test_key, value=test_checkpoint_configuration)
        if pretty_print:
            print(f"\tTest key {test_key} successfully added to Checkpoint store.\n")

        if pretty_print:
            print(
                f"Attempting to retrieve the test value associated with key {test_key} from Checkpoint store..."
            )

        self.get(key=test_key)
        if pretty_print:
            print("\tTest value successfully retrieved from Checkpoint store.")
            print()

        if pretty_print:
            print(f"Cleaning up test key {test_key} and value from Checkpoint store...")

        self.remove_key(key=test_key)
        if pretty_print:
            print("\tTest key and value successfully removed from Checkpoint store.")
            print()

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
        key: Union[GXCloudIdentifier, ConfigurationIdentifier] = self._determine_key(
            name=name, id=id
        )
        try:
            self.remove_key(key=key)
        except gx_exceptions.InvalidKeyError as exc_ik:
            raise gx_exceptions.CheckpointNotFoundError(
                message=f'Non-existent Checkpoint configuration named "{key.configuration_key}".\n\nDetails: {exc_ik}'  # type: ignore[union-attr]
            )

    def get_checkpoint(
        self, name: Optional[ConfigurationIdentifier | str], id: Optional[str]
    ) -> CheckpointConfig:
        key: GXCloudIdentifier | ConfigurationIdentifier
        if not isinstance(name, ConfigurationIdentifier):
            key = self._determine_key(name=name, id=id)
        else:
            key = name

        try:
            checkpoint_config: Optional[Any] = self.get(key=key)
            assert isinstance(
                checkpoint_config, CheckpointConfig
            ), "checkpoint_config retrieved was not of type CheckpointConfig"
        except gx_exceptions.InvalidKeyError as exc_ik:
            raise gx_exceptions.CheckpointNotFoundError(
                message=f'Non-existent Checkpoint configuration named "{key.configuration_key}".\n\nDetails: {exc_ik}'  # type: ignore[union-attr]
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
            return self._persist_checkpoint(
                key=key, checkpoint=checkpoint, persistence_fn=self.add
            )
        except gx_exceptions.StoreBackendError:
            raise gx_exceptions.CheckpointError(
                f"A Checkpoint named {checkpoint.name} already exists."
            )

    def update_checkpoint(
        self, checkpoint: Checkpoint
    ) -> Checkpoint | CheckpointConfig:
        """Use a stand-alone Checkpoint object to update a persisted value.

        Args:
            checkpoint: The Checkpoint to use for updating.

        Returns:
            The persisted Checkpoint (possibly modified state based on store backend).

        Raises:
            CheckpointNotFoundError: If a Checkpoint with the given name does not exist in the store.
        """
        key = self._construct_key_from_checkpoint(checkpoint)
        try:
            return self._persist_checkpoint(
                key=key, checkpoint=checkpoint, persistence_fn=self.update
            )
        except gx_exceptions.StoreBackendError:
            raise gx_exceptions.CheckpointNotFoundError(
                f"Could not find an existing Checkpoint named {checkpoint.name}."
            )

    def add_or_update_checkpoint(
        self, checkpoint: Checkpoint
    ) -> Checkpoint | CheckpointConfig:
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
        id = checkpoint.ge_cloud_id
        if id:
            return self._determine_key(id=id)
        return self._determine_key(name=name)

    def _persist_checkpoint(
        self,
        key: GXCloudIdentifier | ConfigurationIdentifier,
        checkpoint: Checkpoint,
        persistence_fn: Callable,
    ) -> Checkpoint | CheckpointConfig:
        checkpoint_ref = persistence_fn(key=key, value=checkpoint.get_config())
        if isinstance(checkpoint_ref, GXCloudResourceRef):
            # return CheckpointConfig from cloud POST response to account for any defaults/new ids added in cloud
            checkpoint_config = checkpoint_ref.response["data"]["attributes"][
                "checkpoint_config"
            ]
            checkpoint_config["ge_cloud_id"] = checkpoint_config.pop("id")
            return self.deserialize(checkpoint_config)
        elif self.ge_cloud_mode:
            # if in cloud mode and checkpoint_ref is not a GXCloudResourceRef, a PUT operation occurred
            # re-fetch and return CheckpointConfig from cloud to account for any defaults/new ids added in cloud
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
        """
        # CheckpointConfig not an AbstractConfig??
        # mypy error: incompatible type "CheckpointConfig"; expected "AbstractConfig"
        key: DataContextKey = self._build_key_from_config(checkpoint_config)  # type: ignore[arg-type]

        # Make two separate requests to set and get in order to obtain any additional
        # values that may have been added to the config by the StoreBackend (i.e. object ids)
        ref: Optional[Union[bool, GXCloudResourceRef]] = self.set(key, checkpoint_config)  # type: ignore[func-returns-value]
        if ref and isinstance(ref, GXCloudResourceRef):
            key.id = ref.id  # type: ignore[attr-defined]

        config = self.get(key=key)

        return config
