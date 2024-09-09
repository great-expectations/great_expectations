from __future__ import annotations

import numbers
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
    def get_query(cls, query, template_dict, selectable) -> str:
        template_dict_reformatted = {
            k: str(v).format(batch=selectable)
            if isinstance(v, numbers.Number)
            else v.format(batch=selectable)
            for k, v in template_dict.items()
        }
        query_reformatted = query.format(**template_dict_reformatted, batch=selectable)
        return query_reformatted

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

        selectable: Union[sa.sql.Selectable, str]
        selectable, _, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs, domain_type=MetricDomainTypes.TABLE
        )

        if not isinstance(query, str):
            raise TypeError("Query must be supplied as a string")  # noqa: TRY003

        template_dict = metric_value_kwargs.get("template_dict")

        if not isinstance(template_dict, dict):
            raise TypeError("template_dict supplied by the expectation must be a dict")  # noqa: TRY003

        if isinstance(selectable, sa.Table):
            query = cls.get_query(query, template_dict, selectable)

        elif isinstance(
            selectable, get_sqlalchemy_subquery_type()
        ):  # Specifying a runtime query in a RuntimeBatchRequest returns the active batch as a Subquery; sectioning  # noqa: E501
            # the active batch off w/ parentheses ensures flow of operations doesn't break
            query = cls.get_query(query, template_dict, f"({selectable})")

        elif isinstance(
            selectable, sa.sql.Select
        ):  # Specifying a row_condition returns the active batch as a Select object, requiring compilation &  # noqa: E501
            # aliasing when formatting the parameterized query
            query = cls.get_query(
                query,
                template_dict,
                f'({selectable.compile(compile_kwargs={"literal_binds": True})}) AS subselect',
            )

        else:
            query = cls.get_query(query, template_dict, f"({selectable})")

        try:
            result: List[sqlalchemy.Row] = execution_engine.execute_query(sa.text(query)).fetchall()  # type: ignore[assignment,arg-type]
        except Exception as e:
            if hasattr(e, "_query_id"):
                # query_id removed because it duplicates the validation_results
                e._query_id = None
            raise e  # noqa: TRY201

        return [element._asdict() for element in result]

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
        template_dict = metric_value_kwargs.get("template_dict")
        if not isinstance(query, str):
            raise TypeError("template_dict supplied by the expectation must be a dict")  # noqa: TRY003
        if not isinstance(template_dict, dict):
            raise TypeError("template_dict supplied by the expectation must be a dict")  # noqa: TRY003

        query = query.format(**template_dict, batch="tmp_view")

        engine: pyspark.SparkSession = execution_engine.spark
        result: List[pyspark.Row] = engine.sql(query).collect()

        return [element.asDict() for element in result]
