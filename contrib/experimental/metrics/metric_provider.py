from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Union

from pydantic import ConfigDict
from contrib.experimental.metrics.mp_asset import MPBatchDefinition, MPBatchParameters
from great_expectations.compatibility import pydantic

MetricT = TypeVar("MetricT", bound="Metric")
MetricValueT = TypeVar("MetricValueT", bound="MetricValue")
MetricProviderT = TypeVar("MetricProviderT", bound="MetricProvider")


MetricValue = Union[str, int, float, list[str], list[int], list[float], None]

class Metric(ABC, pydantic.BaseModel):
    model_config = ConfigDict(frozen=True)

    """
    Implementation notes:
    Why a batch_definition here and not a batch? Because the batch is a runtime concept, 
    while the batch_definition is a configuration concept. That way, we know how to
    store/retrieve the metric. When paired with the batch_parameters, the batch is
    uniquely identified.

    Question: should we annotate the return type in some way here? Currently it's only
    part of the MetricImplementation
    Bill - THe MetricImplementation makes sense as the thing that does that joining, since it consumes the metric
    maybe the current "Metric" type is a "MetricRequest" or "MetricSpecification" or "MetricKey" or similar
    """
    name: str

    # TODO: does it make more sense to have just batch_definition & batch_parameters
    # here, or would it be better to group these into a single object that is the
    # metric domain, which can then be extended to include the column, column pair,
    # table, and multicolumn cases...and critically perhaps also the case of the
    # row condition.

    # I am leaning to thinking of this as a "domain" because that would also replace the
    # concept of just the batch definition that is currently in the metric domain.
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
    
    def get_supported_metrics(self) -> list[type[Metric]]:
        return list(self._metric_implementations._metric_implementations.keys())
    
    @property
    @abstractmethod
    def supported_batch_definition_types(self) -> list[type[MPBatchDefinition]]:
        raise NotImplementedError

class MetricImplementation(ABC, Generic[MetricT, MetricProviderT, MetricValueT]):
    def __init_subclass__(cls, metric_t: type[Metric], metric_provider_t: type[MetricProvider]) -> None:
        metric_provider_t._metric_implementations.register_metric(metric_t = metric_t, metric_impl_t = cls)

    def __init__(self, metric: MetricT, provider: MetricProviderT) -> None:
        self._metric = metric
        self._provider = provider

    @abstractmethod
    def compute(self) -> MetricValueT:
        pass
