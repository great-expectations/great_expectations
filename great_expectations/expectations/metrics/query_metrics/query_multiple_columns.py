from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List

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
from great_expectations.expectations.metrics.util import MAX_RESULT_RECORDS

if TYPE_CHECKING:
    from great_expectations.compatibility import pyspark


class QueryMultipleColumns(QueryMetricProvider):
    metric_name = "query.multiple_columns"
    value_keys = (
        "columns",
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
        columns = metric_value_kwargs.get("columns")
        if not isinstance(columns, list):
            raise TypeError("Columns must be supplied as a list")  # noqa: TRY003
        return cls._get_sqlalchemy_records_from_query_and_batch_selectable(
            query=query,
            batch_selectable=batch_selectable,
            execution_engine=execution_engine,
            query_parameters=QueryParameters(columns=columns),
        )

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ) -> list[dict]:
        query = cls._get_query_from_metric_value_kwargs(metric_value_kwargs)

        if not isinstance(query, str):
            raise TypeError("Query must be supplied as a string")  # noqa: TRY003

        df: pyspark.DataFrame
        df, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        df.createOrReplaceTempView("tmp_view")
        columns = metric_value_kwargs.get("columns")

        if not isinstance(columns, list):
            raise TypeError("Columns must be supplied as a list")  # noqa: TRY003

        query = query.format(
            **{f"col_{i}": entry for i, entry in enumerate(columns, 1)},
            batch="tmp_view",
        )

        engine: pyspark.SparkSession = execution_engine.spark
        result: List[pyspark.Row] = engine.sql(query).limit(MAX_RESULT_RECORDS).collect()

        return [element.asDict() for element in result]
