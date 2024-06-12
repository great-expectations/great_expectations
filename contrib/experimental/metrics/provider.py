from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar
from psycopg2.extensions import connection
from pydantic import BaseModel


M = TypeVar("M", bound="Metric")
MP = TypeVar("MP", bound="MetricProvider")


class Batch(Generic[MP]):
    def get_metric_domain(self, metric_provider: MP) -> Any:
        return metric_provider.get_metric_domain_from_batch(self)



class Metric(ABC, Generic[MP]):
    def __init__(self, batch: Batch[MP]) -> None:
        self._batch = batch

    @property
    def batch(self) -> Batch[MP]:
        return self._batch



class MetricImplementation(ABC, Generic[M, MP]):
    def __init__(self, metric: M, provider: MP) -> None:
        self._metric = metric
        self._provider = provider

    @abstractmethod
    def compute(self):
        pass



class MetricProvider(ABC):
    def __init__(self) -> None:
        self._provider_metrics: dict

    @abstractmethod
    def get_metric(self, metric: Metric) -> MetricImplementation:
        pass

    @abstractmethod
    def get_metric_domain_from_batch(self, batch: Batch):
        pass


class ColumnMeanMetric(Metric[MP]):
    def __init__(self, batch: Batch[MP], column: str) -> None:
        super().__init__(batch=batch)
        self._column = column

    @property
    def column(self) -> str:
        return self._column



class SnowflakeConnectionMetricProviderBatch(Batch["SnowflakeConnectionMetricProvider"]):
    def __init__(self, selectable: str):
        self._selectable = selectable

    @property
    def selectable(self) -> str:
        return self._selectable



class SnowflakeConnectionMetricProvider(MetricProvider):
    def __init__(self, connection: connection):
        self._connection = connection
        self._metric_implementations: dict[type[Metric], type[MetricImplementation]] = dict()

    def get_metric(self, metric: Metric):
        metric_impl = self._metric_implementations.get(type(metric))
        if metric_impl is None:
            raise ValueError(f"Metric {metric} is not supported by provider {self}")
        return metric_impl(metric=metric, provider=self).compute()
    
    def get_metric_domain_from_batch(self, batch: SnowflakeConnectionMetricProviderBatch) -> str:
        return batch.selectable
        
    def get_connection(self) -> connection:
        return self._connection


class SnowflakeCursorColumnMeanProviderMetric(MetricImplementation[ColumnMeanMetric, SnowflakeConnectionMetricProvider]):
    def compute(self):
        selectable = self._metric.batch.get_metric_domain(self._provider)
        with self._provider.get_connection().cursor() as cursor:
            cursor.execute(f"SELECT AVG({self._metric.column}) FROM {selectable}")
            return cursor.fetchone()


class NewExpectMeanToBeBetween(BaseModel):
    column: str
    min_value: float
    max_value: float

    def validate(self, mean: ColumnMeanMetric):
        metric = ColumnMeanMetric(batch=batch, column=self.column)
        provider = batch.get_metric_provider()
        metric_impl = provider.get_metric(metric)
        value = metric_impl.compute()
        if not (self.min_value <= value <= self.max_value):
            raise ValueError(f"Value {value} is not between {self.min_value} and {self.max_value}")