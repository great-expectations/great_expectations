from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer
from great_expectations.data_context.store.store_backend import StoreBackend

if TYPE_CHECKING:
    from great_expectations.core.usage_statistics.anonymizers.anonymizer import (
        Anonymizer,
    )


class StoreBackendAnonymizer(BaseAnonymizer):
    def __init__(
        self,
        aggregate_anonymizer: Anonymizer,
        salt: Optional[str] = None,
    ) -> None:
        super().__init__(salt=salt)

        self._aggregate_anonymizer = aggregate_anonymizer

    def anonymize(  # type: ignore[override] # different signature from parent class
        self,
        obj: Optional[object] = None,
        store_backend_obj: Optional[StoreBackend] = None,
        store_backend_object_config: Optional[dict] = None,
    ) -> dict:
        assert (
            store_backend_obj or store_backend_object_config
        ), "Must pass store_backend_obj or store_backend_object_config."
        anonymized_info_dict: dict = {}
        if store_backend_obj is not None:
            self._anonymize_object_info(
                object_=store_backend_obj,
                anonymized_info_dict=anonymized_info_dict,
            )
        elif store_backend_object_config is None:
            raise ValueError(
                "Must pass `store_backend_obj` or `store_backend_object_config`."
            )
        else:
            class_name = store_backend_object_config.get("class_name")
            module_name = store_backend_object_config.get("module_name")
            if module_name is None:
                module_name = "great_expectations.data_context.store"
            self._anonymize_object_info(
                object_config={"class_name": class_name, "module_name": module_name},
                anonymized_info_dict=anonymized_info_dict,
            )
        return anonymized_info_dict

    def can_handle(self, obj: Optional[object] = None, **kwargs) -> bool:
        return (obj is not None and isinstance(obj, StoreBackend)) or (
            "store_backend_object_config" in kwargs
        )
