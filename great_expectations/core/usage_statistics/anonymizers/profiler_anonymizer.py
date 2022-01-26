from typing import Optional

from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.rule_based_profiler import RuleBasedProfiler


class ProfilerAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super().__init__(salt=salt)

        # ordered bottom up in terms of inheritance order
        self._ge_classes = [RuleBasedProfiler]

    def anonymize_profiler_info(self, name: str, config: dict) -> dict:
        anonymized_info_dict: dict = {
            "anonymized_name": self.anonymize(name),
        }
        self.anonymize_object_info(
            anonymized_info_dict=anonymized_info_dict,
            ge_classes=self._ge_classes,
            object_config=config,
        )
        return anonymized_info_dict

    def is_parent_class_recognized(self, config) -> Optional[str]:
        return self._is_parent_class_recognized(
            classes_to_check=self._ge_classes,
            object_config=config,
        )
