from great_expectations.core.logging.anonymizer import Anonymizer
from great_expectations.data_context.store import (
    Store,
    ValidationsStore,
    ExpectationsStore,
    HtmlSiteStore,
    MetricStore,
    EvaluationParameterStore,
    StoreBackend,
    InMemoryStoreBackend,
    TupleFilesystemStoreBackend,
    TupleS3StoreBackend,
    TupleGCSStoreBackend,
    DatabaseStoreBackend,
    TupleStoreBackend
)


class StoreAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super(StoreAnonymizer, self).__init__(salt=salt)
        self.ge_store_top_level_classes = [
            Store,
            HtmlSiteStore
        ]
        self.ge_store_child_classes = [
            ValidationsStore,
            ExpectationsStore,
            MetricStore,
            EvaluationParameterStore
        ]
        self.ge_store_backend_top_level_classes = [
            StoreBackend
        ]
        # ordered bottom up in terms of inheritance order
        self.ge_store_backend_child_classes = [
            InMemoryStoreBackend,
            DatabaseStoreBackend,
            TupleFilesystemStoreBackend,
            TupleS3StoreBackend,
            TupleGCSStoreBackend,
            TupleStoreBackend,
        ]

    def anonymize_store_info(self, store_name, store_obj):
        store_class = store_obj.__class__
        store_class_name = store_class.__name__
        store_backend_class = store_obj.store_backend.__class__
        store_backend_class_name = store_backend_class.__name__

        anonymized_store_info = dict()
        anonymized_store_info["anonymized_name"] = self.anonymize(store_name)

        # store info
        for ge_store_child_class in self.ge_store_child_classes:
            if issubclass(store_class, ge_store_child_class):
                anonymized_store_info["parent_class"] = ge_store_child_class.__name__
                if not store_class == ge_store_child_class:
                    anonymized_store_info["anonymized_class"] = self.anonymize(store_class_name)
                break

        if not anonymized_store_info.get("parent_class"):
            for ge_store_parent_class in self.ge_store_top_level_classes:
                if issubclass(store_class, ge_store_parent_class):
                    anonymized_store_info["parent_class"] = ge_store_parent_class.__name__
                    if not store_class == ge_store_parent_class:
                        anonymized_store_info["anonymized_class"] = self.anonymize(store_class_name)
                    break

        if not anonymized_store_info.get("parent_class"):
            anonymized_store_info["parent_class"] = "__not_recognized__"
            anonymized_store_info["anonymized_class"] = self.anonymize(store_class_name)

        # store backend info
        for ge_store_backend_child_class in self.ge_store_backend_child_classes:
            if issubclass(store_backend_class, ge_store_backend_child_class):
                anonymized_store_info["store_backend_parent_class"] = ge_store_backend_child_class.__name__
                if not store_backend_class == ge_store_backend_child_class:
                    anonymized_store_info["anonymized_store_backend_class"] = self.anonymize(store_backend_class_name)
                break

        if not anonymized_store_info.get("store_backend_parent_class"):
            for ge_store_backend_parent_class in self.ge_store_backend_top_level_classes:
                if issubclass(store_backend_class, ge_store_backend_parent_class):
                    anonymized_store_info["store_backend_parent_class"] = ge_store_backend_parent_class.__name__
                    if not store_backend_class == ge_store_backend_parent_class:
                        anonymized_store_info["anonymized_store_backend_class"] = self.anonymize(store_backend_class_name)
                    break

        if not anonymized_store_info.get("store_backend_parent_class"):
            anonymized_store_info["store_backend_parent_class"] = "__not_recognized__"
            anonymized_store_info["anonymized_store_backend_class"] = self.anonymize(store_backend_class_name)

        return anonymized_store_info


