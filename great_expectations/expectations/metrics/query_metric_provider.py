from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar

from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
from great_expectations.expectations.metrics.metric_provider import MetricProvider

if TYPE_CHECKING:
    from great_expectations.compatibility.sqlalchemy import (
        sqlalchemy as sa,
    )

logger = logging.getLogger(__name__)


class QueryMetricProvider(MetricProvider):
    """Base class for all Query Metrics, which define metrics to construct SQL queries.

     An example of this is `query.table`,
     which takes in a SQL query & target table name, and returns the result of that query.

     In some cases, subclasses of MetricProvider, such as QueryMetricProvider, will already
     have correct values that may simply be inherited by Metric classes.

     ---Documentation---
         - https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_query_expectations

    Args:
         metric_name (str): A name identifying the metric. Metric Name must be globally unique in
             a great_expectations installation.
         domain_keys (tuple): A tuple of the keys used to determine the domain of the metric.
         value_keys (tuple): A tuple of the keys used to determine the value of the metric.
         query (str): A valid SQL query.
    """

    domain_keys = ("batch_id", "row_condition", "condition_parser")

    query_param_name: ClassVar[str] = "query"

    dialect_columns_require_subquery_aliases: ClassVar[set[GXSqlDialect]] = {
        GXSqlDialect.POSTGRESQL
    }

    @classmethod
    def _get_query_from_metric_value_kwargs(cls, metric_value_kwargs: dict) -> str:
        query_param = cls.query_param_name
        query: str | None = metric_value_kwargs.get(query_param) or cls.default_kwarg_values.get(
            query_param
        )
        if not query:
            raise ValueError(f"Must provide `{query_param}` to `{cls.__name__}` metric.")  # noqa: TRY003

        return query

    @classmethod
    def _get_query_string_with_substituted_batch_parameters(
        cls, query: str, batch_selectable: sa.sql.Subquery | sa.sql.Alias
    ) -> str:
        """Specifying a runtime query string returns the active batch as a Subquery or Alias type
        There is no object-based way to apply the subquery alias to columns in the SELECT and
        WHERE clauses. Instead, we extract the subquery parameters from the batch selectable
        and inject them into the SQL string.
        """

        batch_table = batch_selectable.selectable.element.get_final_froms()[0].name
        unfiltered_query = query.format(batch=batch_table)
        batch_filter = str(batch_selectable.selectable.element.whereclause)

        if "WHERE" in query.upper():
            # Add a new WHERE condition
            query = unfiltered_query.replace("WHERE", f"WHERE {batch_filter} AND")
        elif "GROUP BY" in query.upper():
            # If there is no existing WHERE clause, but there is a GROUP BY clause
            # add the WHERE clause before the GROUP BY clause
            query = unfiltered_query.replace("GROUP BY", f"WHERE {batch_filter} GROUP BY")
        elif "ORDER BY" in query.upper():
            # If there is no existing WHERE clause, but there is an ORDER BY clause
            # add the WHERE clause before the ORDER BY clause
            query = unfiltered_query.replace("ORDER BY", f"WHERE {batch_filter} ORDER BY")
        else:
            query += f"WHERE {batch_filter}"

        return query
