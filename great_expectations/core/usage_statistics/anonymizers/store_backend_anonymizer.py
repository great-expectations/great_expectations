from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer


class StoreBackendAnonymizer(BaseAnonymizer):
    pass

    # def _anonymize_store_backend_info(
    #     self,
    #     store_backend_obj: Optional[object] = None,
    #     store_backend_object_config: Optional[dict] = None,
    # ) -> dict:
    #     assert (
    #         store_backend_obj or store_backend_object_config
    #     ), "Must pass store_backend_obj or store_backend_object_config."
    #     anonymized_info_dict = {}
    #     if store_backend_obj is not None:
    #         self._anonymize_object_info(
    #             object_=store_backend_obj,
    #             anonymized_info_dict=anonymized_info_dict,
    #         )
    #     else:
    #         class_name = store_backend_object_config.get("class_name")
    #         module_name = store_backend_object_config.get("module_name")
    #         if module_name is None:
    #             module_name = "great_expectations.data_context.store"
    #         self._anonymize_object_info(
    #             object_config={"class_name": class_name, "module_name": module_name},
    #             anonymized_info_dict=anonymized_info_dict,
    #         )
    #     return anonymized_info_dict
