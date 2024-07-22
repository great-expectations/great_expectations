from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar
from great_expectations.compatibility import pydantic
from great_expectations.datasource.fluent.batch_request import BatchParameters

MetricT = TypeVar("MetricT", bound="Metric")
MetricProviderT = TypeVar("MetricProviderT", bound="MetricProvider")
MPPartitionerT = TypeVar("MPPartitionerT", bound="MPPartitioner")
MPAssetT = TypeVar("MPAssetT", bound="MPAsset")

class MPPartitioner(pydantic.BaseModel):
    pass

class MPAsset(pydantic.BaseModel):
    id: Optional[str] = None
    name: str
    _batch_definitions: list["MPBatchDefinition"] = pydantic.PrivateAttr(default_factory=list)

    def add_batch_definition(self, batch_definition: "MPBatchDefinition"):
        self._batch_definitions.append(batch_definition)

    def get_batch_definitions(self, name: str):
        return next(batch_definition for batch_definition in self._batch_definitions if batch_definition.name == name)

class MPTableAsset(MPAsset):
    table_name: str

class MPBatchDefinition(pydantic.GenericModel, Generic[MPAssetT, MPPartitionerT]):
    id: Optional[str] = None
    name: str
    data_asset: MPAssetT
    partitioner: MPPartitionerT

class Metric(pydantic.BaseModel):
    batch_definition: MPBatchDefinition
    batch_parameters: BatchParameters

class ColumnMetric(Metric):
    column: str

class MetricImplementation(ABC, Generic[MetricT, MetricProviderT]):
    def __init__(self, metric: MetricT, provider: MetricProviderT) -> None:
        self._metric = metric
        self._provider = provider

    @abstractmethod
    def compute(self):
        pass

class MetricProvider(ABC):
    def __init__(self) -> None:
        self._metric_implementations: dict[type[Metric], type[MetricImplementation]] = dict()

    @abstractmethod
    def get_metric(self, metric: Metric) -> MetricImplementation:
        pass

    # TODO: automatic registration
    def register_metric(self, metric_t: type[Metric], metric_impl_t: type[MetricImplementation]):
        self._metric_implementations[metric_t] = metric_impl_t

