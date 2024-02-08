from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, Union

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

    # The name of the parameter that will be used to pass the unexpected_rows_query to the query metric provider
    _query_param: ClassVar[str] = "query"

    @classmethod
    def _get_query_from_metric_value_kwargs(cls, metric_value_kwargs: dict) -> str:
        query: str | None = metric_value_kwargs.get(
            cls._query_param
        ) or cls.default_kwarg_values.get(cls._query_param)
        if not query:
            raise ValueError(
                f"Must provide `{cls._query_param}` to `{cls.metric_name}` metric."
            )

        return query

    # <snippet>
    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(  # noqa: PLR0913
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ) -> List[dict]:
        query = cls._get_query_from_metric_value_kwargs(metric_value_kwargs)

        selectable: Union[sa.sql.Selectable, str]
        selectable, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        if isinstance(selectable, sa.Table):
            query = query.format(active_batch=selectable)
        elif isinstance(
            selectable, get_sqlalchemy_subquery_type()
        ):  # Specifying a runtime query in a RuntimeBatchRequest returns the active batch as a Subquery or Alias; sectioning the active batch off w/ parentheses ensures flow of operations doesn't break
            query = query.format(active_batch=f"({selectable})")
        elif isinstance(
            selectable, sa.sql.Select
        ):  # Specifying a row_condition returns the active batch as a Select object, requiring compilation & aliasing when formatting the parameterized query
            query = query.format(
                active_batch=f'({selectable.compile(compile_kwargs={"literal_binds": True})}) AS subselect',
            )
        else:
            query = query.format(active_batch=f"({selectable})")

        result: List[sqlalchemy.Row] = execution_engine.execute_query(
            sa.text(query)
        ).fetchall()
        return [element._asdict() for element in result]
        # </snippet>

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(  # noqa: PLR0913
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
        query = query.format(active_batch="tmp_view")

        engine: pyspark.SparkSession = execution_engine.spark
        result: List[pyspark.Row] = engine.sql(query).collect()

        return [element.asDict() for element in result]
