from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List

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
)
from great_expectations.util import get_sqlalchemy_subquery_type

if TYPE_CHECKING:
    from great_expectations.compatibility import pyspark, sqlalchemy


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
    ) -> List[dict]:
        query = cls._get_query_from_metric_value_kwargs(metric_value_kwargs)

        batch_selectable: sa.sql.Selectable
        batch_selectable, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        if isinstance(batch_selectable, sa.Table):
            query = query.format(batch=batch_selectable)
        elif isinstance(batch_selectable, get_sqlalchemy_subquery_type()):
            if execution_engine.dialect_name in cls.dialect_columns_require_subquery_aliases:
                query = cls._get_query_string_with_substituted_batch_parameters(
                    query=query,
                    batch_selectable=batch_selectable,
                )
            else:
                query = query.format(
                    batch=f'({batch_selectable.compile(compile_kwargs={"literal_binds": True})})'
                )
        elif isinstance(
            batch_selectable, sa.sql.Select
        ):  # Specifying a row_condition returns the active batch as a Select object, requiring compilation & aliasing when formatting the parameterized query  # noqa: E501
            query = query.format(
                batch=f'({batch_selectable.compile(compile_kwargs={"literal_binds": True})}) AS subselect',  # noqa: E501
            )
        else:
            query = query.format(batch=f"({batch_selectable})")

        result: List[sqlalchemy.Row] = execution_engine.execute_query(sa.text(query)).fetchall()  # type: ignore[assignment,arg-type]
        return [element._asdict() for element in result]
        # </snippet>

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
        query = query.format(batch="tmp_view")

        engine: pyspark.SparkSession = execution_engine.spark
        result: List[pyspark.Row] = engine.sql(query).collect()

        return [element.asDict() for element in result]
