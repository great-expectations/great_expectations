from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer


class StoreAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super().__init__(salt=salt)

    def anonymize_store_info(self, store_name, store_obj):
        anonymized_info_dict = {}
        anonymized_info_dict["anonymized_name"] = self.anonymize(store_name)
        store_backend_obj = store_obj.store_backend

        self.anonymize_object_info(
            object_=store_obj,
            anonymized_info_dict=anonymized_info_dict,
        )

        anonymized_info_dict[
            "anonymized_store_backend"
        ] = self.anonymize_store_backend_info(store_backend_obj=store_backend_obj)

        return anonymized_info_dict
