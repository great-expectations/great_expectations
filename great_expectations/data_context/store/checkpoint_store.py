import logging
import random

from great_expectations.data_context.store import ConfigurationStore
from great_expectations.data_context.types.base import CheckpointConfig
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
)

logger = logging.getLogger(__name__)


class CheckpointStore(ConfigurationStore):
    """
    A CheckpointStore manages Checkpoints for the DataContext.
    """

    _configuration_class = CheckpointConfig

    def serialization_self_check(self, pretty_print: bool):
        test_checkpoint_name: str = "test-name-" + "".join(
            [random.choice(list("0123456789ABCDEF")) for i in range(20)]
        )
        test_checkpoint_configuration: CheckpointConfig = CheckpointConfig(
            **{"name": test_checkpoint_name}
        )
        test_key: ConfigurationIdentifier = self._key_class(
            configuration_key=test_checkpoint_name
        )

        if pretty_print:
            print(f"Attempting to add a new test key {test_key} to Checkpoint store...")
        self.set(key=test_key, value=test_checkpoint_configuration)
        if pretty_print:
            print(f"\tTest key {test_key} successfully added to Checkpoint store.")
            print()

        if pretty_print:
            print(
                f"Attempting to retrieve the test value associated with key {test_key} from Checkpoint store..."
            )
        # noinspection PyUnusedLocal
        test_value: CheckpointConfig = self.get(key=test_key)
        if pretty_print:
            print("\tTest value successfully retreived from Checkpoint store.")
            print()

        if pretty_print:
            print(f"Cleaning up test key {test_key} and value from Checkpoint store...")

        # noinspection PyUnusedLocal
        test_value: CheckpointConfig = self.remove_key(key=test_key)
        if pretty_print:
            print("\tTest key and value successfully removed from Checkpoint store.")
            print()
