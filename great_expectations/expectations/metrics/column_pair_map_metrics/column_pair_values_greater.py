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
        # A comparison with a NULL operand is NULL, and so is its negation (the unexpected
        # condition), so the row would count as neither expected nor unexpected. Requiring both
        # operands makes it FALSE instead, and the row is unexpected, as on pandas.
        both_present = sa.and_(column_A.is_not(None), column_B.is_not(None))
        if or_equal:
            return sa.or_(
                sa.and_(column_A >= column_B, both_present),
                sa.and_(column_A == None, column_B == None),  # noqa: E711 # FIXME CoP
            )
        else:
            return sa.and_(column_A > column_B, both_present)

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
