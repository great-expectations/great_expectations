from abc import ABC, abstractmethod
import hashlib
import json
from typing import Any, Generic, TypeVar, Union

from pydantic import ConfigDict, StrictStr
from great_expectations.compatibility import pydantic

MetricT = TypeVar("MetricT", bound="Metric")
MetricReturnT = TypeVar("MetricReturnT", bound="MetricValue")
MetricProviderT = TypeVar("MetricProviderT", bound="MetricProvider")
MPPartitionerT = TypeVar("MPPartitionerT", bound="MPPartitioner")
MPAssetT = TypeVar("MPAssetT", bound="MPAsset")


MetricValue = Union[str, int, float, list[str], list[int], list[float], None]

class MPPartitioner(pydantic.BaseModel):
    model_config = ConfigDict(frozen=True)


class MPAsset(pydantic.BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str
    name: str
    _batch_definitions: list["MPBatchDefinition"] = pydantic.PrivateAttr(default_factory=list)

    def add_batch_definition(self, batch_definition: "MPBatchDefinition") -> None:
        self._batch_definitions.append(batch_definition)

    def get_batch_definition(self, name: str) -> "MPBatchDefinition":
        return next(batch_definition for batch_definition in self._batch_definitions if batch_definition.name == name)

class MPBatchParameters(dict[StrictStr, Any]):
    @property
    def id(self) -> str:
        return hashlib.md5(json.dumps(self, sort_keys=True).encode("utf-8")).hexdigest()

class MPTableAsset(MPAsset):
    table_name: str

class MPBatchDefinition(pydantic.GenericModel, Generic[MPAssetT, MPPartitionerT]):
    model_config = ConfigDict(frozen=True)

    id: str
    name: str
    data_asset: MPAssetT
    partitioner: MPPartitionerT

    def get_id(self, batch_parameters: MPBatchParameters) -> str:
        # TODO: quick-and-dirty naive implementation
        return "__".join((
            self.data_asset.id,
            self.id,
            batch_parameters.id
        ))

class Metric(ABC, pydantic.BaseModel):
    model_config = ConfigDict(frozen=True)

    """
    Implementation notes:
    Why a batch_definition here and not a batch? Because the batch is a runtime concept, 
    while the batch_definition is a configuration concept. That way, we know how to
    store/retrieve the metric.

    Question: should we annotate the return type in some way here? Currently it's only
    part of the MetricImplementation
    """
    name: str

    batch_definition: MPBatchDefinition
    batch_parameters: MPBatchParameters

    @property
    @abstractmethod
    def id(self):
        pass

class MPMetricImplementationRegistry:
    def __init__(self, ) -> None:
        self._metric_implementations: dict[type[Metric], type[MetricImplementation]] = dict()

    def register_metric(self, metric_t: type[Metric], metric_impl_t: type["MetricImplementation"]):
        self._metric_implementations[metric_t] = metric_impl_t

    def get_metric_implementation(self, provider: "MetricProvider", metric: Metric) -> type["MetricImplementation"]:
        metric_impl = self._metric_implementations.get(type(metric))
        if metric_impl is None:
            raise ValueError(f"Metric {metric} is not supported by provider {provider}")
        return metric_impl


class MetricProvider(ABC):
    _metric_implementations: MPMetricImplementationRegistry = MPMetricImplementationRegistry()

    def is_supported_batch_definition(self, batch_definition: MPBatchDefinition) -> bool:
        return type(batch_definition) in self.supported_batch_definition_types

    def get_metric_implementation(self, metric: Metric) -> "MetricImplementation":
        metric_impl = self._metric_implementations.get_metric_implementation(self, metric)
        return metric_impl(metric=metric, provider=self)
    
    @property
    @abstractmethod
    def supported_batch_definition_types(self) -> list[type[MPBatchDefinition]]:
        raise NotImplementedError

class MetricImplementation(ABC, Generic[MetricT, MetricProviderT, MetricReturnT]):
    """
    Q: is MetricReturnT useful here?
    """
    def __init_subclass__(cls, metric_t: type[Metric], metric_provider_t: type[MetricProvider]) -> None:
        metric_provider_t._metric_implementations.register_metric(metric_t = metric_t, metric_impl_t = cls)

    def __init__(self, metric: MetricT, provider: MetricProviderT) -> None:
        self._metric = metric
        self._provider = provider

    @abstractmethod
    def compute(self) -> MetricReturnT:
        pass
