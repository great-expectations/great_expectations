from contrib.experimental.metrics.column_metric import ColumnMeanMetric, ColumnValuesMatchRegexMetric
from contrib.experimental.metrics.column_metric import (
    ColumnValuesMatchRegexUnexpectedValuesMetric,
)
from contrib.experimental.metrics.metric_provider import (
    MetricImplementation,
    MetricProvider,
)
from contrib.experimental.metrics.domain import MetricDomain
from contrib.experimental.metrics.snowflake_provider_domain import (
    SnowflakeSelectableDomain,
)

# TODO: maybe replace with gx compatibility layer, but I think that may be anchored to sqlalchemy
import snowflake
import snowflake.connector


class SnowflakeConnectionMetricProvider(
    MetricProvider
):
    def __init__(
        self,
        connection: snowflake.connector.SnowflakeConnection,
    ):
        super().__init__()
        self._connection = connection

    def get_connection(
        self,
    ) -> (
        snowflake.connector.SnowflakeConnection
    ):
        return self._connection

    def get_selectable(
        self,
        domain: SnowflakeSelectableDomain
    ) -> str:
        return domain.get_selectable_str(
            batch_parameters=domain.batch_parameters,
        )


class SnowflakeColumnMeanMetricImplementation(
    MetricImplementation[
        ColumnMeanMetric,
        SnowflakeConnectionMetricProvider,
        float | None,
    ],
    metric_t=ColumnMeanMetric,
    metric_provider_t=SnowflakeConnectionMetricProvider,
):
    def compute(self) -> float | None:
        selectable = self._provider.get_selectable(
            self._metric.domain,
        )
        with self._provider.get_connection().cursor() as cursor:
            cursor.execute(
                f"SELECT AVG({self._metric.domain.column}) FROM {selectable}"
            )
            res = cursor.fetchone()
            if res is None:
                return None
            else:
                return res[0]


class SnowflakeColumnValuesMatchRegexMetricImplementation(
    MetricImplementation[
        ColumnValuesMatchRegexMetric,
        SnowflakeConnectionMetricProvider,
        int,
    ],
    metric_t=ColumnValuesMatchRegexMetric,
    metric_provider_t=SnowflakeConnectionMetricProvider,
):
    def compute(self) -> int:
        selectable = self._provider.get_selectable(
            self._metric.domain
        )
        # TODO: This highlights where we might want to use a metric dependency
        # e.g. on value counts -- otherwise we have a separate metric for the
        # count, for the ratio, etc.
        with self._provider.get_connection().cursor() as cursor:
            cursor.execute(
                f"SELECT COUNT(*) FROM {selectable} WHERE {self._metric.domain.column} NOT REGEXP '{self._metric.regex}'"
            )
            res = cursor.fetchone()
            if res is None:
                return 0
            return res[0]


class SnowflakeColumnValuesMatchRegexUnexpectedValuesMetricImplementation(
    MetricImplementation[
        ColumnValuesMatchRegexUnexpectedValuesMetric,
        SnowflakeConnectionMetricProvider,
        int,
    ],
    metric_t=ColumnValuesMatchRegexUnexpectedValuesMetric,
    metric_provider_t=SnowflakeConnectionMetricProvider,
):
    def compute(self) -> list[str]:
        selectable = self._provider.get_selectable(
            self._metric.domain
        )
        with self._provider.get_connection().cursor() as cursor:
            cursor.execute(
                f"SELECT {self._metric.domain.column} FROM {selectable} WHERE {self._metric.domain.column} NOT REGEXP '{self._metric.regex}' LIMIT {self._metric.limit}"
            )
            res = cursor.fetchall()
            if res is None:
                return []
            return [row[0] for row in res]
