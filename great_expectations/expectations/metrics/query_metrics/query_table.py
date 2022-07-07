from typing import Any, Dict, List, Union

from great_expectations.execution_engine import (
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.expectations.metrics.import_manager import sa
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.query_metric_provider import (
    QueryMetricProvider,
)


class QueryTable(QueryMetricProvider):
    metric_name = "query.table"
    value_keys = ("query",)

    # <snippet>
    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ) -> Union[List[Any], Any]:
        query = metric_value_kwargs.get("query") or cls.default_kwarg_values.get(
            "query"
        )

        engine = execution_engine.engine
        active_batch, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        if isinstance(active_batch, sa.sql.schema.Table):
            query = query.format(active_batch=active_batch)
        elif isinstance(
            active_batch, sa.sql.selectable.Subquery
        ):  # Specifying a runtime query in a RuntimeBatchRequest returns the active bacth as a Subquery; sectioning the active batch off w/ parentheses ensures flow of operations doesn't break
            query = query.format(active_batch=f"({active_batch})")
        elif isinstance(
            active_batch, sa.sql.selectable.Select
        ):  # Specifying a row_condition returns the active batch as a Select object, requiring compilation & aliasing when formatting the parameterized query
            query = query.format(
                active_batch=f'({active_batch.compile(compile_kwargs={"literal_binds": True})}) AS subselect',
            )
        else:
            query = query.format(active_batch=f"({active_batch})")

        result = engine.execute(sa.text(query)).fetchall()

        return result
        # </snippet>

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ) -> Union[List[Any], Any]:
        query = metric_value_kwargs.get("query") or cls.default_kwarg_values.get(
            "query"
        )

        engine = execution_engine.spark
        active_batch, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        active_batch.createOrReplaceTempView("active_batch")
        query = query.format(active_batch="active_batch")

        result = engine.sql(query).collect()

        return result
