from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Sequence, Union

from typing_extensions import NotRequired, TypedDict

from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
from great_expectations.expectations.metrics.metric_provider import MetricProvider
from great_expectations.expectations.metrics.util import MAX_RESULT_RECORDS
from great_expectations.util import get_sqlalchemy_subquery_type

if TYPE_CHECKING:
    from great_expectations.execution_engine import SqlAlchemyExecutionEngine

logger = logging.getLogger(__name__)


class MissingElementError(TypeError):
    def __init__(self):
        super().__init__(
            "The batch subquery selectable does not contain an "
            "element from which query parameters can be extracted."
        )


class QueryParameters(TypedDict):
    column: NotRequired[str]
    column_A: NotRequired[str]
    column_B: NotRequired[str]
    columns: NotRequired[list[str]]


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
        if not isinstance(query, str):
            raise TypeError(f"`{query_param}` must be provided as a string.")  # noqa: TRY003

        return query

    @classmethod
    def _get_query_string_with_substituted_batch_parameters(
        cls, query: str, batch_subquery: sa.sql.Subquery | sa.sql.Alias
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
            froms = batch_subquery.selectable.element.get_final_froms()  # type: ignore[attr-defined]  # possible AttributeError handled
            try:
                batch_table = froms[0].name
            except AttributeError:
                batch_table = str(froms[0])
            batch_filter = str(batch_subquery.selectable.element.whereclause)  # type: ignore[attr-defined]  # possible AttributeError handled
        except (AttributeError, IndexError) as e:
            raise MissingElementError() from e

        unfiltered_query = query.format(batch=batch_table)

        if "WHERE" in query.upper():
            query = unfiltered_query.replace("WHERE", f"WHERE {batch_filter} AND")
        elif "GROUP BY" in query.upper():
            query = unfiltered_query.replace("GROUP BY", f"WHERE {batch_filter} GROUP BY")
        elif "ORDER BY" in query.upper():
            query = unfiltered_query.replace("ORDER BY", f"WHERE {batch_filter} ORDER BY")
        else:
            query = unfiltered_query + f" WHERE {batch_filter}"

        return query

    @classmethod
    def _get_parameters_dict_from_query_parameters(
        cls, query_parameters: Optional[QueryParameters]
    ) -> dict[str, Any]:
        if not query_parameters:
            return {}
        elif query_parameters and "columns" in query_parameters:
            columns = query_parameters.pop("columns")
            query_columns = {f"col_{i}": col for i, col in enumerate(columns, 1)}
            return {**query_parameters, **query_columns}
        else:
            return {**query_parameters}

    @classmethod
    def _get_sqlalchemy_records_from_query_and_batch_selectable(
        cls,
        query: str,
        batch_selectable: sa.Selectable,
        execution_engine: SqlAlchemyExecutionEngine,
        query_parameters: Optional[QueryParameters] = None,
    ) -> list[dict]:
        parameters = cls._get_parameters_dict_from_query_parameters(query_parameters)

        if isinstance(batch_selectable, sa.Table):
            query = query.format(batch=batch_selectable, **parameters)
        elif isinstance(batch_selectable, get_sqlalchemy_subquery_type()):
            if (
                not parameters
                and execution_engine.dialect_name in cls.dialect_columns_require_subquery_aliases
            ):
                try:
                    query = cls._get_query_string_with_substituted_batch_parameters(
                        query=query,
                        batch_subquery=batch_selectable,
                    )
                except MissingElementError:
                    # if we are unable to extract the subquery parameters,
                    # we fall back to the default behavior for all dialects
                    batch = batch_selectable.compile(compile_kwargs={"literal_binds": True})
                    query = query.format(batch=f"({batch})", **parameters)
            else:
                batch = batch_selectable.compile(compile_kwargs={"literal_binds": True})
                query = query.format(batch=f"({batch})", **parameters)
        elif isinstance(
            batch_selectable, sa.sql.Select
        ):  # Specifying a row_condition returns the active batch as a Select object
            # requiring compilation & aliasing when formatting the parameterized query
            batch = batch_selectable.compile(compile_kwargs={"literal_binds": True})
            query = query.format(batch=f"({batch}) AS subselect", **parameters)
        else:
            query = query.format(batch=f"({batch_selectable})", **parameters)

        result: Union[Sequence[sa.Row[Any]], Any] = execution_engine.execute_query(
            sa.text(query)  # type: ignore[arg-type]
        ).fetchmany(MAX_RESULT_RECORDS)

        if isinstance(result, Sequence):
            return [element._asdict() for element in result]
        else:
            return [result]
