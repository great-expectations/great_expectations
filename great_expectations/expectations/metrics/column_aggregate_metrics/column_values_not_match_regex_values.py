from __future__ import annotations

import logging
from types import ModuleType
from typing import Any

from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.expectations.metrics.column_aggregate_metric_provider import (
    ColumnAggregateMetricProvider,
)
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.expectations.metrics.util import get_dialect_regex_expression

logger = logging.getLogger(__name__)


class ColumnValuesNotMatchRegexValues(ColumnAggregateMetricProvider):
    metric_name = "column_values.not_match_regex_values"

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(  # FIXME CoP
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: dict[str, Any],
        runtime_configuration: dict,
    ) -> list[str]:
        # Prefer the dialect module, but fall back to the live dialect: `_setup_dialect` resolves
        # no module for some backends (Exasol among them) and leaves `dialect_module` None.
        # `SqlAlchemyExecutionEngine.dialect` is the property that returns `self.engine.dialect`.
        _dialect: ModuleType | sa.Dialect | None = execution_engine.dialect_module
        if _dialect is None:
            _dialect = execution_engine.dialect

        (
            selectable,
            _,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(metric_domain_kwargs, MetricDomainTypes.COLUMN)
        column_name: str = accessor_domain_kwargs["column"]
        column: sa.ColumnClause = sa.column(column_name)
        regex = metric_value_kwargs["regex"]
        limit = metric_value_kwargs["limit"]

        regex_expression = get_dialect_regex_expression(column, regex, _dialect)
        if regex_expression is None:
            dialect_name = getattr(getattr(_dialect, "dialect", _dialect), "name", str(_dialect))
            msg = f"Regex is not supported for dialect {dialect_name}"
            logger.warning(msg)
            raise NotImplementedError(msg)

        # Find values that DO NOT match the regex
        query = sa.select(column).where(sa.not_(regex_expression)).select_from(selectable)  # type: ignore[arg-type]
        if isinstance(limit, int):
            query = query.limit(limit)

        rows = execution_engine.execute_query(query).fetchall()

        return [row[0] for row in rows]
