from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.core.usage_statistics.anonymizers.store_backend_anonymizer import (
    StoreBackendAnonymizer,
)


class StoreAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super().__init__(salt=salt)
        self._store_backend_anonymizer = StoreBackendAnonymizer(salt=salt)

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
        ] = self._store_backend_anonymizer.anonymize_store_backend_info(
            store_backend_obj=store_backend_obj
        )

        return anonymized_info_dict

    def get_parent_class(self, store_obj):
        return self._get_parent_class(object_=store_obj)
