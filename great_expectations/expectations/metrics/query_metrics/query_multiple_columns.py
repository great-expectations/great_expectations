from typing import TYPE_CHECKING, Any, Dict, List, Union

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


class QueryMultipleColumns(QueryMetricProvider):
    metric_name = "query.multiple_columns"
    value_keys = (
        "columns",
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
        query = metric_value_kwargs.get("query") or cls.default_kwarg_values.get(
            "query"
        )

        if not isinstance(query, str):
            raise TypeError("Query must be supplied as a string")

        selectable: Union[sa.sql.Selectable, str]
        selectable, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        columns = metric_value_kwargs.get("columns")

        if not isinstance(columns, list):
            raise TypeError("Columns must be supplied as a list")

        if isinstance(selectable, sa.Table):
            query = query.format(
                **{f"col_{i}": entry for i, entry in enumerate(columns, 1)},
                active_batch=selectable,
            )
        elif isinstance(
            selectable, get_sqlalchemy_subquery_type()
        ):  # Specifying a runtime query in a RuntimeBatchRequest returns the active bacth as a Subquery; sectioning the active batch off w/ parentheses ensures flow of operations doesn't break
            query = query.format(
                **{f"col_{i}": entry for i, entry in enumerate(columns, 1)},
                active_batch=f"({selectable})",
            )
        elif isinstance(
            selectable, sa.sql.Select
        ):  # Specifying a row_condition returns the active batch as a Select object, requiring compilation & aliasing when formatting the parameterized query
            query = query.format(
                **{f"col_{i}": entry for i, entry in enumerate(columns, 1)},
                active_batch=f'({selectable.compile(compile_kwargs={"literal_binds": True})}) AS subselect',
            )
        else:
            query = query.format(
                **{f"col_{i}": entry for i, entry in enumerate(columns, 1)},
                active_batch=f"({selectable})",
            )

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
        query = metric_value_kwargs.get("query") or cls.default_kwarg_values.get(
            "query"
        )

        if not isinstance(query, str):
            raise TypeError("Query must be supplied as a string")

        df: pyspark.DataFrame
        df, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        df.createOrReplaceTempView("tmp_view")
        columns = metric_value_kwargs.get("columns")

        if not isinstance(columns, list):
            raise TypeError("Columns must be supplied as a list")

        query = query.format(
            **{f"col_{i}": entry for i, entry in enumerate(columns, 1)},
            active_batch="tmp_view",
        )

        engine: pyspark.SparkSession = execution_engine.spark
        result: List[pyspark.Row] = engine.sql(query).collect()

        return [element.asDict() for element in result]
