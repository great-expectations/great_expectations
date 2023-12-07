from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

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


class QueryColumn(QueryMetricProvider):
    metric_name = "query.column"
    value_keys = (
        "column",
        "query",
    )

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(  # noqa: PLR0913
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ) -> List[dict]:
        query: Optional[str] = metric_value_kwargs.get(
            "query"
        ) or cls.default_kwarg_values.get("query")

        selectable: Union[sa.sql.Selectable, str]
        selectable, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        column: Optional[str] = metric_value_kwargs.get("column")
        if isinstance(selectable, sa.Table):
            query = query.format(col=column, active_batch=selectable)  # type: ignore[union-attr] # could be none
        elif isinstance(
            selectable, get_sqlalchemy_subquery_type()
        ):  # Specifying a runtime query in a RuntimeBatchRequest returns the active bacth as a Subquery; sectioning the active batch off w/ parentheses ensures flow of operations doesn't break
            query = query.format(col=column, active_batch=f"({selectable})")  # type: ignore[union-attr] # could be none
        elif isinstance(
            selectable, sa.sql.Select
        ):  # Specifying a row_condition returns the active batch as a Select object, requiring compilation & aliasing when formatting the parameterized query
            query = query.format(  # type: ignore[union-attr] # could be none
                col=column,
                active_batch=f'({selectable.compile(compile_kwargs={"literal_binds": True})}) AS subselect',
            )
        else:
            query = query.format(col=column, active_batch=f"({selectable})")  # type: ignore[union-attr] # could be none

        result: List[sqlalchemy.Row] = execution_engine.execute_query(
            sa.text(query)
        ).fetchall()

        return [element._asdict() for element in result]

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(  # noqa: PLR0913
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ) -> List[dict]:
        query: Optional[str] = metric_value_kwargs.get(
            "query"
        ) or cls.default_kwarg_values.get("query")

        df: pyspark.DataFrame
        df, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        df.createOrReplaceTempView("tmp_view")
        column: Optional[str] = metric_value_kwargs.get("column")
        query = query.format(col=column, active_batch="tmp_view")  # type: ignore[union-attr] # could be none

        engine: pyspark.SparkSession = execution_engine.spark
        result: List[pyspark.Row] = engine.sql(query).collect()

        return [element.asDict() for element in result]
