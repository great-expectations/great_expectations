import logging
import os
import random
import uuid
from typing import Dict, List, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context.store import ConfigurationStore
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    DataContextConfigDefaults,
)
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GeCloudIdentifier,
)

logger = logging.getLogger(__name__)


class CheckpointStore(ConfigurationStore):
    """
    A CheckpointStore manages Checkpoints for the DataContext.
    """

    _configuration_class = CheckpointConfig

    def ge_cloud_response_json_to_object_dict(self, response_json: Dict) -> Dict:
        """
        This method takes full json response from GE cloud and outputs a dict appropriate for
        deserialization into a GE object
        """
        ge_cloud_checkpoint_id = response_json["data"]["id"]
        checkpoint_config_dict = response_json["data"]["attributes"][
            "checkpoint_config"
        ]
        checkpoint_config_dict["ge_cloud_id"] = ge_cloud_checkpoint_id

        return checkpoint_config_dict

    def serialization_self_check(self, pretty_print: bool):
        test_checkpoint_name: str = "test-name-" + "".join(
            [random.choice(list("0123456789ABCDEF")) for i in range(20)]
        )
        test_checkpoint_configuration: CheckpointConfig = CheckpointConfig(
            **{"name": test_checkpoint_name}
        )
        if self.ge_cloud_mode:
            test_key: GeCloudIdentifier = self.key_class(
                resource_type="contract", ge_cloud_id=str(uuid.uuid4())
            )
        else:
            test_key: ConfigurationIdentifier = self.key_class(
                configuration_key=test_checkpoint_name
            )

        if pretty_print:
            print(f"Attempting to add a new test key {test_key} to Checkpoint store...")
        self.set(key=test_key, value=test_checkpoint_configuration)
        if pretty_print:
            print(f"\tTest key {test_key} successfully added to Checkpoint store.\n")

        if pretty_print:
            print(
                f"Attempting to retrieve the test value associated with key {test_key} from Checkpoint store..."
            )
        # noinspection PyUnusedLocal
        test_value: CheckpointConfig = self.get(key=test_key)
        if pretty_print:
            print("\tTest value successfully retrieved from Checkpoint store.")
            print()

        if pretty_print:
            print(f"Cleaning up test key {test_key} and value from Checkpoint store...")

        # noinspection PyUnusedLocal
        test_value: CheckpointConfig = self.remove_key(key=test_key)
        if pretty_print:
            print("\tTest key and value successfully removed from Checkpoint store.")
            print()

    @staticmethod
    def default_checkpoints_exist(directory_path: str) -> bool:
        if not directory_path:
            return False

        checkpoints_directory_path: str = os.path.join(
            directory_path,
            DataContextConfigDefaults.DEFAULT_CHECKPOINT_STORE_BASE_DIRECTORY_RELATIVE_NAME.value,
        )
        return os.path.isdir(checkpoints_directory_path)

    def list_checkpoints(self, ge_cloud_mode: bool) -> List[str]:
        keys: Union[List[str], List[ConfigurationIdentifier]] = self.list_keys()
        if ge_cloud_mode:
            return keys
        return [k.configuration_key for k in keys]

    def delete_checkpoint(
        self,
        name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
    ) -> None:
        assert bool(name) ^ bool(
            ge_cloud_id
        ), "Must provide either name or ge_cloud_id."

        key: Union[GeCloudIdentifier, ConfigurationIdentifier]
        if ge_cloud_id:
            key = GeCloudIdentifier(resource_type="contract", ge_cloud_id=ge_cloud_id)
        else:
            key = ConfigurationIdentifier(configuration_key=name)

        try:
            self.remove_key(key=key)
        except ge_exceptions.InvalidKeyError as exc_ik:
            raise ge_exceptions.CheckpointNotFoundError(
                message=f'Non-existent Checkpoint configuration named "{key.configuration_key}".\n\nDetails: {exc_ik}'
            )
