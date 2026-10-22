from __future__ import annotations

import logging

from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.expectations.metrics.util import (
    get_dialect_like_pattern_expression,
)

logger = logging.getLogger(__name__)


class ColumnValuesNotMatchLikePattern(ColumnMapMetricProvider):
    condition_metric_name = "column_values.not_match_like_pattern"
    condition_value_keys = (
        "like_pattern",
        "escape",
    )

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, like_pattern, _dialect, escape=None, **kwargs):
        like_pattern_expression = get_dialect_like_pattern_expression(
            column, _dialect, like_pattern, positive=False, escape=escape
        )
        if like_pattern_expression is None:
            dialect = getattr(_dialect, "dialect", _dialect)
            message = f"Like patterns are not supported for dialect {dialect.name!s}"
            logger.warning(message)
            raise NotImplementedError(message)

        return like_pattern_expression
