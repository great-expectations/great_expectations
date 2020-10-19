from typing import Any, Dict, Tuple

from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_map_metric import (
    ColumnMapMetric,
    column_map_condition,
    map_condition,
)
from great_expectations.expectations.metrics.metric import metric


class ColumnPairValuesEqual(ColumnMapMetric):
    condition_metric_name = "column_pair_values.equal"
    domain_keys = ("batch_id", "table", "column_a", "column_b")

    @map_condition(engine=PandasExecutionEngine, bundle_metric=False)
    def _pandas(
        cls,
        execution_engine: "PandasExecutionEngine",
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: dict,
    ):
        df, compute_domain, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs
        )
        return (
            df[metric_domain_kwargs["column_a"]] == df[metric_domain_kwargs["column_b"]]
        )

    @column_map_condition(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, value_set, **kwargs):
        return column.in_(value_set)

    @column_map_condition(engine=SparkDFExecutionEngine)
    def _spark(cls, column, value_set, **kwargs):
        return column.isin(value_set)
