from contrib.experimental.metrics.metric import (
    ColumnMeanMetric,
    ColumnValuesMatchRegexMetric,
    ColumnValuesMatchRegexUnexpectedValuesMetric,
)
from contrib.experimental.metrics.metric_provider import (
    MetricImplementation,
    MetricProvider,
)
from contrib.experimental.metrics.mp_asset import MPBatchParameters
from contrib.experimental.metrics.snowflake_provider_batch_definition import (
    SnowflakeMPBatchDefinition,
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
        batch_definition: SnowflakeMPBatchDefinition,
        batch_parameters: MPBatchParameters,
    ) -> str:
        return batch_definition.get_selectable_str(
            batch_parameters=batch_parameters
        )

    @property
    def supported_batch_definition_types(
        self,
    ) -> list[type]:
        return [SnowflakeMPBatchDefinition]

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
            self._metric.batch_definition,
            self._metric.batch_parameters,
        )
        with self._provider.get_connection().cursor() as cursor:
            cursor.execute(
                f"SELECT AVG({self._metric.column}) FROM {selectable}"
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
            self._metric.batch_definition,
            self._metric.batch_parameters,
        )
        # TODO: This highlights where we might want to use a metric dependency
        # e.g. on value counts -- otherwise we have a separate metric for the
        # count, for the ratio, etc.
        with self._provider.get_connection().cursor() as cursor:
            cursor.execute(
                f"SELECT COUNT(*) FROM {selectable} WHERE {self._metric.column} REGEXP '{self._metric.regex}'"
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
            self._metric.batch_definition,
            self._metric.batch_parameters,
        )
        with self._provider.get_connection().cursor() as cursor:
            cursor.execute(
                f"SELECT {self._metric.column} FROM {selectable} WHERE {self._metric.column} REGEXP '{self._metric.regex}' LIMIT {self._metric.limit}"
            )
            res = cursor.fetchall()
            if res is None:
                return []
            return [row[0] for row in res]
