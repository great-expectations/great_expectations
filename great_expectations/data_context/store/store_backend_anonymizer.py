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


class StoreBackendAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super(StoreBackendAnonymizer, self).__init__(salt=salt)

        # ordered bottom up in terms of inheritance order
        self._ge_classes = [
            TupleFilesystemStoreBackend,
            TupleS3StoreBackend,
            TupleGCSStoreBackend,
            InMemoryStoreBackend,
            DatabaseStoreBackend,
            TupleStoreBackend,
            StoreBackend
        ]

    def anonymize_store_backend_info(self, store_backend_obj):
        anonymized_info_dict = dict()
        self.anonymize_object_info(
            object_=store_backend_obj,
            anonymized_info_dict=anonymized_info_dict,
            ge_classes=self._ge_classes
        )
        return anonymized_info_dict