from typing import TypeAlias, Union

from great_expectations.experimental.column_descriptive_metrics.metrics import Metrics

CloudStorableTypes: TypeAlias = Union[Metrics,]  # TODO: are there better approaches?


class CloudDataStore:
    def __init__(
        self,
    ):
        # TODO: Get credentials from Cloud Data Context on init
        pass

    def create(
        self, value_type: CloudStorableTypes, value: CloudStorableTypes
    ) -> None:  # TODO: How to annotate?
        # TODO: implementation
        print(
            f"Creating item of type {value_type.__name__} in CloudStoreBackend - sending a POST REST API request to the cloud."
        )
        print(value)
