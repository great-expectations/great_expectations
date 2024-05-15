from __future__ import annotations

from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnPairMapMetricProvider,
    column_pair_condition_partial,
)


class ColumnPairValuesAGreaterThanB(ColumnPairMapMetricProvider):
    condition_metric_name = "column_pair_values.a_greater_than_b"
    condition_domain_keys = (
        "batch_id",
        "table",
        "column_A",
        "column_B",
        "row_condition",
        "condition_parser",
        "ignore_row_if",
    )
    condition_value_keys = ("or_equal",)

    # noinspection PyPep8Naming
    @column_pair_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_A, column_B, **kwargs):
        temp_column_A = column_A
        temp_column_B = column_B

        or_equal: bool = kwargs.get("or_equal") or False
        if or_equal:
            return temp_column_A >= temp_column_B
        else:
            return temp_column_A > temp_column_B

    # noinspection PyPep8Naming
    @column_pair_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column_A, column_B, **kwargs):
        or_equal: bool = kwargs.get("or_equal") or False
        if or_equal:
            return sa.or_(
                column_A >= column_B,
                sa.and_(column_A == None, column_B == None),  # noqa: E711
            )
        else:
            return column_A > column_B

    # noinspection PyPep8Naming
    @column_pair_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column_A, column_B, **kwargs):
        temp_column_A = column_A
        temp_column_B = column_B

        or_equal: bool = kwargs.get("or_equal") or False
        if or_equal:
            return (temp_column_A >= temp_column_B) | (temp_column_A.eqNullSafe(temp_column_B))
        else:
            return temp_column_A > temp_column_B
