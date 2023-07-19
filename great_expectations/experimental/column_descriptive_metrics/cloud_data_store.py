from typing import Union

from typing_extensions import TypeAlias

from great_expectations.data_context import CloudDataContext
from great_expectations.experimental.column_descriptive_metrics.metrics import Metrics

CloudStorableTypes: TypeAlias = Union[Metrics,]  # TODO: are there better approaches?


class CloudDataStore:
    def __init__(self, context: CloudDataContext):
        # TODO: Get credentials from Cloud Data Context on init
        self._context = context
        pass

    def create(
        self, value_type: CloudStorableTypes, value: CloudStorableTypes
    ) -> None:  # TODO: How to annotate?
        # TODO: implementation
        # TODO: Serialize with organization_id from the context
        print(
            f"Creating item of type {value_type.__name__} in CloudStoreBackend - sending a POST REST API request to the cloud."
        )
        print(value)
