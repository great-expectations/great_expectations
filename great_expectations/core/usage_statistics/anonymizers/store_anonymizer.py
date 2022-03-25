from typing import Any, Optional

from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer
from great_expectations.data_context.store.store import Store


class StoreAnonymizer(BaseAnonymizer):
    def anonymize(self, store_name: str, store_obj: Store) -> Any:
        anonymized_info_dict = {}
        anonymized_info_dict["anonymized_name"] = self._anonymize_string(store_name)
        store_backend_obj = store_obj.store_backend

        self._anonymize_object_info(
            object_=store_obj,
            anonymized_info_dict=anonymized_info_dict,
        )

        anonymized_info_dict[
            "anonymized_store_backend"
        ] = self._anonymize_store_backend_info(store_backend_obj=store_backend_obj)

        return anonymized_info_dict

    def _anonymize_store_backend_info(
        self,
        store_backend_obj: Optional[object] = None,
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
    def can_handle(obj: object, **kwargs) -> bool:
        from great_expectations.data_context.store.store import Store

        return (obj and isinstance(obj, Store)) or (
            "store_name" in kwargs or "store_obj" in kwargs
        )
