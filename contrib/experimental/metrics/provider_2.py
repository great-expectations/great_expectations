from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from psycopg2.extensions import connection
from pydantic import BaseModel

from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.datasource.fluent.batch_request import BatchParameters
from great_expectations.datasource.fluent.interfaces import DataAsset
from great_expectations.datasource.fluent.sql_datasource import TableAsset


M = TypeVar("M", bound="Metric")
MP = TypeVar("MP", bound="MetricProvider")


class Metric(BaseModel):
    batch_definition: BatchDefinition
    batch_parameters: BatchParameters

class ColumnMetric(Metric):
    column: str

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


class ColumnMeanMetric(Metric):
    column: str


class SnowflakeTableAsset(TableAsset):
    pass


class SnowflakeConnectionMetricProvider(MetricProvider):
    def __init__(self, connection: connection):
        self._connection = connection
        self._metric_implementations: dict[type[Metric], type[MetricImplementation]] = dict()

    def get_metric(self, metric: Metric):
        metric_impl = self._metric_implementations.get(type(metric))
        if metric_impl is None:
            raise ValueError(f"Metric {metric} is not supported by provider {self}")
        return metric_impl(metric=metric, provider=self).compute()
    
    def get_connection(self) -> connection:
        return self._connection
    
    def get_selectable(
            self, 
            batch_definition: BatchDefinition,
            batch_parameters: BatchParameters
        ) -> str:
        return batch_definition.get_selectable_str(batch_parameters=batch_parameters)


class SnowflakeColumnMeanMetricImplementation(MetricImplementation[ColumnMeanMetric, SnowflakeConnectionMetricProvider]):
    def compute(self):
        selectable = self._provider.get_selectable(self._metric.batch_definition, self._metric.batch_parameters)
        with self._provider.get_connection().cursor() as cursor:
            cursor.execute(f"SELECT AVG({self._metric.column}) FROM {selectable}")
            return cursor.fetchone()

