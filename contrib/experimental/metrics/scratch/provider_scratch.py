from abc import ABC, abstractmethod



class BatchMetric(ABC):
    def __init__(self, batch: "Batch"):
        self._batch = batch
        self._value_is_cached = False
        self._value = None

    def get_value(self, provider_controller: "ProviderController") -> Any:
        """Implmenntations should specify the return type more precisely."""
        if self._value_is_cached:
            return self._value
        provider_metric = provider_controller.get_metric(self)
        value = provider_metric.get_value(self._column_name)
        self._value = value
        self._value_is_cached = True
        return value


class ProviderMetric(ABC):

    @property
    @abstractmethod
    def metric(self) -> type[BatchMetric]:
        pass

    @property
    @abstractmethod
    def provider(self) -> type["MetricProvider"]:
        pass

    def register(self):
        get_metric_provider_registry().register(self)


class ProviderMetricRegistry(ABC):
    @abstractmethod
    def register(self, metric: ProviderMetric) -> None:
        pass

    @abstractmethod
    def get_available_providers(self, metric: BatchMetric) -> list[type[ProviderMetric]]:
        pass





class InMemoryMetricProviderRegistry(ProviderMetricRegistry):
    def __init__(self):
        self._metrics: dict[type[BatchMetric], list[type[ProviderMetric]]] = dict()

    def register(self, provider_metric: type[ProviderMetric]) -> None:
        if provider_metric.metric not in self._metrics:
            self._metrics[provider_metric.metric] = []
        self._metrics[provider_metric.metric].append(provider_metric)

    def get_available_providers(self, metric: BatchMetric) -> list[type[ProviderMetric]]:
        return self._metrics.get(metric)


def get_metric_provider_registry() -> ProviderMetricRegistry:
    return InMemoryMetricProviderRegistry()






class MetricProvider:
    def __init__(self, metric_registry: ProviderMetricRegistry):
        self._metric_registry = metric_registry

    def register_metric(self, metric: ProviderMetric):
        if metric.provider != self.__class__:
            raise ValueError("Metric provider mismatch")
        self._metric_registry.register(metric)
    
    @abstractmethod
    def get_metric(self, metric: BatchMetric):
        pass


class SnowflakeMetricProvider(MetricProvider):
    pass


class ColumnMean(BatchMetric):
    def __init__(self, batch: Batch, column_name: str):
        super().__init__(batch)
        self._column_name = column_name

    def get_value(self, provider_controller: "ProviderController") -> float:
        return super().get_value(provider_controller)


class SnowflakeColumnMean(ProviderMetric):
    @property
    def metric(self) -> type[ColumnMean]:
        return ColumnMean

    @property
    def provider(self) -> type[SnowflakeMetricProvider]:
        return SnowflakeMetricProvider

    def get_value(self) -> float:
        return 


class ProviderController:
    @abstractmethod
    def get_metric(self, metric: BatchMetric) -> ProviderMetric:
        pass
    
class FirstAvailableProviderController(ProviderController):
    def get_metric(self, metric: BatchMetric) -> ProviderMetric:
        # Find available providers
        available_metric_providers = get_metric_provider_registry().get_available_providers(metric)

        # Select the best provider
        metric_provider = available_metric_providers[0]

        return get_metric_provider_registry().get_provider_metric(metric)


ColumnMean("foo").compute(provider_controller)

