from typing import Any, Dict, Optional

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.table_metric_provider import (
    TableMetricProvider,
)
from great_expectations.validator.metric_configuration import MetricConfiguration


class TableColumnCount(TableMetricProvider):
    metric_name = "table.column_count"

    @metric_value(engine=PandasExecutionEngine)
    def _pandas(  # noqa: PLR0913
        cls,
        execution_engine: "ExecutionEngine",
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ):
        columns = metrics.get("table.columns")
        return len(columns)  # type: ignore[arg-type]

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(  # noqa: PLR0913
        cls,
        execution_engine: "ExecutionEngine",
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ):
        columns = metrics.get("table.columns")
        return len(columns)  # type: ignore[arg-type]

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(  # noqa: PLR0913
        cls,
        execution_engine: "ExecutionEngine",
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ):
        columns = metrics.get("table.columns")
        return len(columns)  # type: ignore[arg-type]

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        dependencies: dict = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )
        table_domain_kwargs: dict = {
            k: v for k, v in metric.metric_domain_kwargs.items() if k != "column"
        }
        dependencies["table.columns"] = MetricConfiguration(
            metric_name="table.columns",
            metric_domain_kwargs=table_domain_kwargs,
            metric_value_kwargs=None,
        )
        return dependencies
