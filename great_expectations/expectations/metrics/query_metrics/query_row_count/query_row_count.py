from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Sequence, Union

from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.execution_engine import (
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.query_metric_provider import (
    QueryMetricProvider,
    render_derived_table_alias,
    strip_top_level_order_by,
)

if TYPE_CHECKING:
    from great_expectations.compatibility import pyspark


class QueryRowCount(QueryMetricProvider):
    metric_name = "query.row_count"
    value_keys = ("query",)

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ) -> int:
        batch_selectable, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )
        query = cls._get_query_from_metric_value_kwargs(metric_value_kwargs)
        substituted_batch_subquery = (
            cls._get_substituted_batch_subquery_from_query_and_batch_selectable(
                query=query,
                batch_selectable=batch_selectable,
                execution_engine=execution_engine,
            )
        )
        if execution_engine.dialect_name == "mssql":
            substituted_batch_subquery = strip_top_level_order_by(substituted_batch_subquery)

        count_column_name = "unexpected_row_count"
        aliased_subquery = render_derived_table_alias(
            subquery=substituted_batch_subquery,
            alias="substituted_batch_subquery",
            dialect_name=execution_engine.dialect_name,
        )
        row_count_query = f"SELECT COUNT(*) as {count_column_name} FROM {aliased_subquery}"
        result: Union[Sequence[sa.Row[Any]], Any] = execution_engine.execute_query(
            sa.text(row_count_query)
        ).fetchone()
        return int(result[0])

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ) -> int:
        query = cls._get_query_from_metric_value_kwargs(metric_value_kwargs)

        df: pyspark.DataFrame
        df, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        df.createOrReplaceTempView("tmp_view")
        query = cls._format_query(query, batch="tmp_view")

        engine: pyspark.SparkSession = execution_engine.spark
        return engine.sql(query).count()
