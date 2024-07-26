from abc import ABC, abstractmethod
from typing import Generic
from contrib.experimental.metrics.metric_provider import MetricProviderT

class MPDataSource(ABC, Generic[MetricProviderT]):
    @abstractmethod
    def get_metric_provider(self) -> MetricProviderT:
        pass
