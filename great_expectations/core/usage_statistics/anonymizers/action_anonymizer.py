from typing import Any, Optional

from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer


class ActionAnonymizer(BaseAnonymizer):
    def anonymize(self, obj: object, **kwargs) -> Any:
        action_name: str = kwargs["action_name"]
        action_obj: Optional[object] = kwargs.get("action_obj")
        action_config: Optional[dict] = kwargs.get("action_config")
        anonymized_info_dict: dict = {
            "anonymized_name": self._anonymize_string(action_name),
        }

        self._anonymize_object_info(
            object_=action_obj,
            object_config=action_config,
            anonymized_info_dict=anonymized_info_dict,
            runtime_environment={"module_name": "great_expectations.checkpoint"},
        )

        return anonymized_info_dict

    @staticmethod
    def can_handle(obj: object, **kwargs) -> bool:
        return "action_name" in kwargs
