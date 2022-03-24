import copy
from typing import Any, Optional, Set, Union

from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer

from great_expectations.core.usage_statistics.anonymizers.types.base import (  # isort:skip
    GETTING_STARTED_DATASOURCE_NAME,
    GETTING_STARTED_EXPECTATION_SUITE_NAME,
    GETTING_STARTED_CHECKPOINT_NAME,
    BATCH_REQUEST_FLATTENED_KEYS,
)


class StoreAnonymizer(BaseAnonymizer):
    def anonymize(self, obj: object = None, **kwargs) -> Any:
        store_name: str = kwargs["store_name"]
        store_obj: object = kwargs["store_obj"]

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

    def _anonymize_batch_request_properties(
        self, source: Optional[Any] = None
    ) -> Optional[Union[str, dict]]:
        if source is None:
            return None

        if isinstance(source, str) and source in BATCH_REQUEST_FLATTENED_KEYS:
            return source

        if isinstance(source, dict):
            source_copy: dict = copy.deepcopy(source)
            anonymized_keys: Set[str] = set()

            key: str
            value: Any
            for key, value in source.items():
                if key in BATCH_REQUEST_FLATTENED_KEYS:
                    if self._is_getting_started_keyword(value=value):
                        source_copy[key] = value
                    else:
                        source_copy[key] = self._anonymize_batch_request_properties(
                            source=value
                        )
                else:
                    anonymized_key: str = self._anonymize_string(key)
                    source_copy[
                        anonymized_key
                    ] = self._anonymize_batch_request_properties(source=value)
                    anonymized_keys.add(key)

            for key in anonymized_keys:
                source_copy.pop(key)

            return source_copy

        return self._anonymize_string(str(source))

    @staticmethod
    def _is_getting_started_keyword(value: str) -> bool:
        return value in [
            GETTING_STARTED_DATASOURCE_NAME,
            GETTING_STARTED_EXPECTATION_SUITE_NAME,
            GETTING_STARTED_CHECKPOINT_NAME,
        ]

    @staticmethod
    def can_handle(obj: object, **kwargs) -> bool:
        from great_expectations.data_context.store.store import Store

        return (obj and isinstance(obj, Store)) or (
            "store_name" in kwargs or "store_obj" in kwargs
        )
