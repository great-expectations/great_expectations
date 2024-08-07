from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Union

from contrib.experimental.metrics.metric import Metric
from contrib.experimental.metrics.domain import MetricDomain

MetricT = TypeVar("MetricT", bound="Metric")
MetricValueT = TypeVar("MetricValueT", bound="MetricValue")
MetricProviderT = TypeVar("MetricProviderT", bound="MetricProvider")

# NOTE: should we differentiate between statistic-type metrics and diagnostic-type metrics?
MetricValue = Union[str, int, float, list[str], list[int], list[float], None]

class MPMetricImplementationRegistry:
    def __init__(self, ) -> None:
        self._metric_implementations: dict[type[Metric], type["MetricImplementation"]] = dict()

    def register_metric(self, metric_t: type[Metric], metric_impl_t: type["MetricImplementation"]):
        self._metric_implementations[metric_t] = metric_impl_t

    def get_metric_implementation(self, provider: "MetricProvider", metric: Metric) -> type["MetricImplementation"]:
        metric_impl = self._metric_implementations.get(type(metric))
        if metric_impl is None:
            raise ValueError(f"Metric {metric} is not supported by provider {provider}")
        return metric_impl

class MPMetricDomainImplementationRegistry:
    def __init__(self, ) -> None:
        self._domain_implementations: dict[type[MetricDomain], type["MetricDomainImplementation"]] = dict()

    def register_domain(self, domain_t: type[MetricDomain], domain_impl_t: type["MetricDomainImplementation"]):
        self._domain_implementations[domain_t] = domain_impl_t

    def get_domain_implementation(self, provider: "MetricProvider", domain: MetricDomain) -> type["MetricDomainImplementation"]:
        domain_impl = self._domain_implementations.get(type(domain))
        if domain_impl is None:
            raise ValueError(f"Domain {domain} is not supported by provider {provider}")
        return domain_impl


class MetricProvider(ABC):
    _metric_implementations: MPMetricImplementationRegistry = MPMetricImplementationRegistry()
    _domain_implementations: MPMetricDomainImplementationRegistry = MPMetricDomainImplementationRegistry()

    @abstractmethod
    def is_supported_domain(self, domain: MetricDomain) -> bool:
        pass

    def get_metric_implementation(self, metric: Metric) -> "MetricImplementation":
        metric_impl = self._metric_implementations.get_metric_implementation(self, metric)
        return metric_impl(metric=metric, provider=self)

    def get_domain_implementation(self, domain: MetricDomain) -> "MetricDomainImplementation":
        domain_impl = self._domain_implementations.get_domain_implementation(self, domain)
        return domain_impl(domain=domain, provider=self)
    
    def get_supported_metrics(self) -> list[type[Metric]]:
        return list(self._metric_implementations._metric_implementations.keys())

class MetricImplementation(ABC, Generic[MetricT, MetricProviderT, MetricValueT]):
    def __init_subclass__(cls, metric_t: type[Metric], metric_provider_t: type[MetricProvider]) -> None:
        metric_provider_t._metric_implementations.register_metric(metric_t = metric_t, metric_impl_t = cls)

    def __init__(self, metric: MetricT, provider: MetricProviderT) -> None:
        self._metric = metric
        self._provider = provider

    @abstractmethod
    def compute(self) -> MetricValueT:
        pass
