from typing import Any, Dict, Optional, Tuple

from sqlalchemy.engine import reflection

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.metric import Metric
from great_expectations.exceptions import GreatExpectationsError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_aggregate_metric import (
    ColumnMetricProvider,
    column_aggregate_metric,
)
from great_expectations.expectations.metrics.column_aggregate_metric import sa as sa
from great_expectations.expectations.metrics.metric_provider import metric
from great_expectations.expectations.metrics.table_metric import (
    TableMetricProvider,
    aggregate_metric,
)
from great_expectations.expectations.metrics.table_metrics.table_column_types import (
    _get_spark_column_metadata,
)
from great_expectations.expectations.metrics.util import column_reflection_fallback
from great_expectations.validator.validation_graph import MetricConfiguration

try:
    import pyspark.sql.types as sparktypes
except ImportError:
    sparktypes = None


class TableColumns(TableMetricProvider):
    metric_name = "table.columns"

    @metric(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        column_metadata = metrics["table.column_types"]
        return [col["name"] for col in column_metadata]

    @metric(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        column_metadata = metrics["table.column_types"]
        return [col["name"] for col in column_metadata]

    @metric(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        column_metadata = metrics["table.column_types"]
        return [col["name"] for col in column_metadata]

    def get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        return {
            "table.column_types": MetricConfiguration(
                "table.column_types",
                metric.metric_domain_kwargs,
                {"include_nested": True},
            )
        }
