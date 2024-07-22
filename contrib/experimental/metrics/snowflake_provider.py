from contrib.experimental.metrics.metric import ColumnMeanMetric
from contrib.experimental.metrics.metric_provider import Metric, MetricImplementation, MetricProvider
from contrib.experimental.metrics.snowflake_provider_batch_definition import SnowflakeMPBatchDefinition
from great_expectations.datasource.fluent.batch_request import BatchParameters

# TODO: maybe replace with gx compatibility layer, but I think that may be anchored to sqlalchemy
import snowflake
import snowflake.connector

class SnowflakeConnectionMetricProvider(MetricProvider):
    def __init__(self, connection: snowflake.connector.SnowflakeConnection):
        super().__init__()
        self._connection = connection

    def get_metric(self, metric: Metric):
        metric_impl = self._metric_implementations.get(type(metric))
        if metric_impl is None:
            raise ValueError(f"Metric {metric} is not supported by provider {self}")
        return metric_impl(metric=metric, provider=self).compute()
    
    def get_connection(self) -> snowflake.connector.SnowflakeConnection:
        return self._connection
    
    def get_selectable(
            self, 
            batch_definition: SnowflakeMPBatchDefinition,
            batch_parameters: BatchParameters
        ) -> str:
        return batch_definition.get_selectable_str(batch_parameters=batch_parameters)


class SnowflakeColumnMeanMetricImplementation(MetricImplementation[ColumnMeanMetric, SnowflakeConnectionMetricProvider]):
    def compute(self):
        selectable = self._provider.get_selectable(self._metric.batch_definition, self._metric.batch_parameters)
        with self._provider.get_connection().cursor() as cursor:
            cursor.execute(f"SELECT AVG({self._metric.column}) FROM {selectable}")
            return cursor.fetchone()

