from abc import ABC, abstractmethod
from contrib.experimental.metrics.metric_provider import Metric, MetricProvider, MetricValue


class MPCache(ABC):
    @abstractmethod
    def get(self, metric: Metric) -> MetricValue:
        pass

    @abstractmethod
    def set(self, metric: Metric, value: MetricValue) -> None:
        pass

class MetricNotComputableError(Exception):
    pass

class InMemoryMPCache(MPCache):
    def __init__(self):
        self._cache = {}

    def get(self, metric: Metric) -> str | int | float |  None:
        return self._cache[metric.id]

    def set(self, metric: Metric, value: str | int | float | None) -> None:
        self._cache[metric.id] = value

class MetricProviderController:
    def __init__(self, metric_providers: list[MetricProvider], caches: list[MPCache]):
        self._metric_providers = metric_providers
        self._caches = caches


    def get_metric(self, metric: Metric):
        for cache in self._caches:
            try:
                value = cache.get(metric)
                return value
            except KeyError:
                continue

        # if it's not in the cache, find a data source that can provide it
        for metric_provider in self._metric_providers:
            if metric_provider.is_supported_batch_definition(metric.batch_definition):
                metric_impl = metric_provider.get_metric_implementation(metric)
                try:
                    value = metric_impl.compute()
                    for cache in self._caches:
                        cache.set(metric, value)
                    return value
                except MetricNotComputableError:
                    continue
            
        raise MetricNotComputableError(f"Could not compute metric.")
