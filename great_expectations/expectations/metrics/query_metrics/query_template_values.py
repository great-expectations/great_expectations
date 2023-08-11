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


class QueryTemplateValues(QueryMetricProvider):
    metric_name = "query.template_values"
    value_keys = (
        "template_dict",
        "query",
    )

    @classmethod
    def get_query(cls, query, template_dict, selectable):
        template_dict_reformatted = {
            k: v.format(active_batch=selectable) for k, v in template_dict.items()
        }
        query_reformatted = query.format(
            **template_dict_reformatted, active_batch=selectable
        )
        return query_reformatted

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

        selectable: Union[sa.sql.Selectable, str]
        selectable, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        if not isinstance(query, str):
            raise TypeError("Query must be supplied as a string")

        template_dict = metric_value_kwargs.get("template_dict")

        if not isinstance(template_dict, dict):
            raise TypeError("template_dict supplied by the expectation must be a dict")

        if isinstance(selectable, sa.Table):
            query = cls.get_query(query, template_dict, selectable)

        elif isinstance(
            selectable, get_sqlalchemy_subquery_type()
        ):  # Specifying a runtime query in a RuntimeBatchRequest returns the active batch as a Subquery; sectioning
            # the active batch off w/ parentheses ensures flow of operations doesn't break
            query = cls.get_query(query, template_dict, f"({selectable})")

        elif isinstance(
            selectable, sa.sql.Select
        ):  # Specifying a row_condition returns the active batch as a Select object, requiring compilation &
            # aliasing when formatting the parameterized query
            query = cls.get_query(
                query,
                template_dict,
                f'({selectable.compile(compile_kwargs={"literal_binds": True})}) AS subselect',
            )

        else:
            query = cls.get_query(query, template_dict, f"({selectable})")

        try:
            result: List[sqlalchemy.Row] = execution_engine.execute_query(
                sa.text(query)
            ).fetchall()
        except Exception as e:
            if hasattr(e, "_query_id"):
                # query_id removed because it duplicates the validation_results
                e._query_id = None
            raise e

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

        df: pyspark.DataFrame
        df, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        df.createOrReplaceTempView("tmp_view")
        template_dict = metric_value_kwargs.get("template_dict")
        if not isinstance(query, str):
            raise TypeError("template_dict supplied by the expectation must be a dict")
        if not isinstance(template_dict, dict):
            raise TypeError("template_dict supplied by the expectation must be a dict")

        query = query.format(**template_dict, active_batch="tmp_view")

        engine: pyspark.SparkSession = execution_engine.spark
        result: List[pyspark.Row] = engine.sql(query).collect()

        return [element.asDict() for element in result]
