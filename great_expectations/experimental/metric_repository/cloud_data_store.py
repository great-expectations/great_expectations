from typing import TypeVar, Union

from typing_extensions import TypeAlias

from great_expectations.experimental.metric_repository.data_store import DataStore
from great_expectations.experimental.metric_repository.metrics import MetricRun

StorableTypes: TypeAlias = Union[MetricRun,]  # TODO: are there better approaches?

T = TypeVar("T", bound=StorableTypes)


class CloudDataStore(DataStore[StorableTypes]):
    def add(self, value: T) -> T:
        # TODO: implementation
        # TODO: Serialize with organization_id from the context
        print(f"Creating item of type {value.__class__.__name__}")
        print(f" in {self.__class__.__name__}")
        print("  sending a POST request to the cloud.")
        print(value)
        return value
