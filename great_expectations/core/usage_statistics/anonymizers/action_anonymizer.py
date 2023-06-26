from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer

if TYPE_CHECKING:
    from great_expectations.core.usage_statistics.anonymizers.anonymizer import (
        Anonymizer,
    )


class ActionAnonymizer(BaseAnonymizer):
    def __init__(
        self,
        aggregate_anonymizer: Anonymizer,
        salt: Optional[str] = None,
    ) -> None:
        super().__init__(salt=salt)

        self._aggregate_anonymizer = aggregate_anonymizer

    def anonymize(  # type: ignore[override] # differs from parent class
        self,
        action_name: str,
        action_obj: Optional[object] = None,
        action_config: Optional[dict] = None,
        obj: Optional[object] = None,
    ) -> Any:
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

    def can_handle(self, obj: Optional[object] = None, **kwargs) -> bool:
        return "action_name" in kwargs
