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
    condition_value_keys = ("like_pattern",)

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, like_pattern, _dialect, **kwargs):
        import inspect

        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any((var in k) for var in ("__frame", "__file", "__func")):
                continue
            print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
        like_pattern_expression = get_dialect_like_pattern_expression(
            column, _dialect, like_pattern, positive=False
        )
        if like_pattern_expression is None:
            logger.warning(
                f"Like patterns are not supported for dialect {str(_dialect.name)}"
            )
            raise NotImplementedError
        return like_pattern_expression
