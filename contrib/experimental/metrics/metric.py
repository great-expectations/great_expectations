from typing import Protocol
from contrib.experimental.metrics.domain import MetricDomain
from contrib.experimental.metrics.domain import ColumnMetricDomain
from great_expectations.compatibility import pydantic



from abc import ABC, abstractmethod


class Metric(Protocol):
    @property
    def name(self) -> str:
        ...

    @property
    def domain(self) -> MetricDomain:
        ...    

    @property
    def id(self) -> str:
        ...


class ColumnMetric(ABC, pydantic.BaseModel):
    domain: ColumnMetricDomain

    @property
    @abstractmethod
    def id(self) -> str:
        pass
