import logging
from typing import ClassVar, Set, Union

from great_expectations.core._docs_decorators import public_api
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
from great_expectations.expectations.metrics.metric_provider import MetricProvider

logger = logging.getLogger(__name__)


@public_api
class MissingElementError(TypeError):
    def __init__(self):
        super().__init__(
            "The batch_subquery selectable does not contain an "
            "element from which query parameters can be extracted."
        )


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

    dialect_columns_require_subquery_aliases: ClassVar[Set[GXSqlDialect]] = {
        GXSqlDialect.POSTGRESQL
    }

    @classmethod
    def _get_query_string_with_substituted_batch_parameters(
        cls,
        query: str,
        batch_subquery: Union[  # type: ignore[name-defined]  # fixed in 1.0
            "sa.sql.Subquery", "sa.sql.Alias"  # noqa: F821  # fixed in 1.0
        ],
    ) -> str:
        """Specifying a runtime query string returns the active batch as a Subquery or Alias type
        There is no object-based way to apply the subquery alias to columns in the SELECT and
        WHERE clauses. Instead, we extract the subquery parameters from the batch selectable
        and inject them into the SQL string.

        Raises:
            MissingElementError if the batch_subquery.selectable does not have an element
            for which to extract query parameters.
        """

        try:
            froms = batch_subquery.selectable.element.get_final_froms()
            try:
                batch_table = froms[0].name
            except AttributeError:
                batch_table = str(froms[0])
            batch_filter = str(batch_subquery.selectable.element.whereclause)
        except (AttributeError, IndexError) as e:
            raise MissingElementError() from e

        unfiltered_query = query.format(batch=batch_table)

        if "WHERE" in query.upper():
            query = unfiltered_query.replace("WHERE", f"WHERE {batch_filter} AND")
        elif "GROUP BY" in query.upper():
            query = unfiltered_query.replace(
                "GROUP BY", f"WHERE {batch_filter} GROUP BY"
            )
        elif "ORDER BY" in query.upper():
            query = unfiltered_query.replace(
                "ORDER BY", f"WHERE {batch_filter} ORDER BY"
            )
        else:
            query = unfiltered_query + f" WHERE {batch_filter}"

        return query
