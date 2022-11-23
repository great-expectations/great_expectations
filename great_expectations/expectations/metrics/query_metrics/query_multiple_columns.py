from typing import Any, Dict, List, Optional, Union

from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.execution_engine import (
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.import_manager import (
    pyspark_sql_DataFrame,
    pyspark_sql_Row,
    pyspark_sql_SparkSession,
    sa,
    sqlalchemy_engine_Engine,
    sqlalchemy_engine_Row,
)
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.query_metric_provider import (
    QueryMetricProvider,
)
from great_expectations.util import get_sqlalchemy_subquery_type


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
    ) -> List[sqlalchemy_engine_Row]:
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

        engine: sqlalchemy_engine_Engine = execution_engine.engine
        result: List[sqlalchemy_engine_Row] = engine.execute(sa.text(query)).fetchall()

        return result

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ) -> List[pyspark_sql_Row]:
        query = metric_value_kwargs.get("query") or cls.default_kwarg_values.get(
            "query"
        )

        if not isinstance(query, str):
            raise TypeError("Query must be supplied as a string")

        df: pyspark_sql_DataFrame
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

        engine: pyspark_sql_SparkSession = execution_engine.spark
        result: List[pyspark_sql_Row] = engine.sql(query).collect()

        return result
