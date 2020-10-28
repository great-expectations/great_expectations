from typing import Any, Dict, Optional, Tuple

from sqlalchemy.engine import reflection

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.metric import Metric
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
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
from great_expectations.expectations.metrics.util import column_reflection_fallback
from great_expectations.validator.validation_graph import MetricConfiguration


class TableColumns(TableMetricProvider):
    metric_name = "table.columns"

    @metric(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: dict,
    ):
        df, _, _ = execution_engine.get_compute_domain(metric_domain_kwargs)
        cols = df.columns
        return cols.tolist()
