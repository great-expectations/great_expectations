from typing import Dict, Any, Tuple

from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.expectations.metrics.metric_provider import metric_partial_fn, metric_value_fn
from great_expectations.expectations.metrics.table_metric import (
    TableMetricProvider,
)


class TableRowCount(TableMetricProvider):
    metric_name = "table.row_count"

    @metric_value_fn(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine: "PandasExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        df, _, _ = execution_engine.get_compute_domain(
            domain_kwargs = metric_domain_kwargs
        )
        return df.shape[0]

    @metric_partial_fn(engine=SqlAlchemyExecutionEngine, partial_fn_type="aggregate_fn", domain_type="table")
    def _sqlalchemy(*args, **kwargs):
        return sa.func.count()

    @metric_partial_fn(engine=SparkDFExecutionEngine, partial_fn_type="aggregate_fn", domain_type="table")
    def _spark(*args, **kwargs):
        return F.count(F.lit(1))
