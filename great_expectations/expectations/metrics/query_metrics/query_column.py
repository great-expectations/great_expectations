from typing import Any, Dict, Optional, Union

from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.expectations.metrics.import_manager import sa
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.query_metric_provider import (
    QueryMetricProvider,
)


class QueryColumn(QueryMetricProvider):
    metric_name = "query.column"
    value_keys = (
        "column",
        "query",
    )

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[str, Any],
        runtime_configuration: Dict,
    ):
        column = metric_value_kwargs.get("column")
        query = metric_value_kwargs.get("query") or cls.default_kwarg_values.get(
            "query"
        )

        engine = execution_engine.engine
        active_batch, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        if isinstance(active_batch, sa.sql.schema.Table):
            query = query.format(col=column, active_batch=active_batch)
        elif isinstance(active_batch, sa.sql.selectable.Subquery):
            query = query.format(col=column, active_batch=f"({active_batch})")
        elif isinstance(active_batch, sa.sql.selectable.Select):
            query = query.format(
                col=column,
                active_batch=f'({active_batch.compile(compile_kwargs={"literal_binds": True})}) AS subselect',
            )
        else:
            query = query.format(col=column, active_batch=f"({active_batch})")

        result = engine.execute(sa.text(query)).fetchall()

        return result
