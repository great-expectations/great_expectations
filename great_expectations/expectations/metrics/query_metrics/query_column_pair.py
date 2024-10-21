from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.execution_engine import (
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.query_metric_provider import (
    QueryMetricProvider,
    QueryParameters,
)

if TYPE_CHECKING:
    from great_expectations.compatibility import pyspark


class QueryColumnPair(QueryMetricProvider):
    metric_name = "query.column_pair"
    value_keys = (
        "column_A",
        "column_B",
        "query",
    )

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ) -> list[dict]:
        batch_selectable, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )
        query = cls._get_query_from_metric_value_kwargs(metric_value_kwargs)
        column_A: Optional[str] = metric_value_kwargs.get("column_A")
        column_B: Optional[str] = metric_value_kwargs.get("column_B")
        if column_A and column_B:
            query_parameters = QueryParameters(
                column_A=column_A,
                column_B=column_B,
            )
        else:
            raise ValueError("Both `column_A` and `column_B` must be provided.")  # noqa: TRY003
        return cls._get_sqlalchemy_records_from_query_and_batch_selectable(
            query=query,
            batch_selectable=batch_selectable,
            execution_engine=execution_engine,
            query_parameters=query_parameters,
        )

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ) -> List[dict]:
        query = cls._get_query_from_metric_value_kwargs(metric_value_kwargs)

        df: pyspark.DataFrame
        df, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        df.createOrReplaceTempView("tmp_view")
        column_A: Optional[str] = metric_value_kwargs.get("column_A")
        column_B: Optional[str] = metric_value_kwargs.get("column_B")
        query = query.format(column_A=column_A, column_B=column_B, batch="tmp_view")

        engine: pyspark.SparkSession = execution_engine.spark
        result: List[pyspark.Row] = engine.sql(query).collect()

        return [element.asDict() for element in result]
