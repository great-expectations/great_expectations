from typing import Optional

from ruamel.yaml.comments import CommentedMap

from great_expectations.checkpoint import Checkpoint, SimpleCheckpoint
from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.data_context.types.base import checkpointConfigSchema


class CheckpointAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super().__init__(salt=salt)

        # ordered bottom up in terms of inheritance order
        self._ge_classes = [SimpleCheckpoint, Checkpoint]

    def anonymize_checkpoint_info(self, name: str, config: dict) -> dict:
        anonymized_info_dict: dict = {
            "anonymized_name": self.anonymize(name),
        }

        # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
        checkpoint_config: dict = checkpointConfigSchema.load(CommentedMap(**config))
        checkpoint_config_dict: dict = checkpointConfigSchema.dump(checkpoint_config)

        self.anonymize_object_info(
            anonymized_info_dict=anonymized_info_dict,
            ge_classes=self._ge_classes,
            object_config=checkpoint_config_dict,
        )
        return anonymized_info_dict

    def is_parent_class_recognized(self, config) -> Optional[str]:
        return self._is_parent_class_recognized(
            classes_to_check=self._ge_classes,
            object_config=config,
        )
