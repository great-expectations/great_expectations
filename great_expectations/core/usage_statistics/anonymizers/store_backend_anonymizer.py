from typing import Optional

from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer
from great_expectations.data_context.store.store_backend import StoreBackend


class StoreBackendAnonymizer(BaseAnonymizer):
    def anonymize(
        self,
        obj: Optional[object] = None,
        store_backend_obj: Optional[StoreBackend] = None,
        store_backend_object_config: Optional[dict] = None,
    ) -> dict:
        assert (
            store_backend_obj or store_backend_object_config
        ), "Must pass store_backend_obj or store_backend_object_config."
        anonymized_info_dict = {}
        if store_backend_obj is not None:
            self._anonymize_object_info(
                object_=store_backend_obj,
                anonymized_info_dict=anonymized_info_dict,
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

    @staticmethod
    def can_handle(obj: Optional[object] = None, **kwargs) -> bool:
        return (obj is not None and isinstance(obj, StoreBackend)) or (
            "store_backend_object_config" in kwargs
        )
