from __future__ import annotations

from great_expectations.compatibility import pyspark
from great_expectations.compatibility.not_imported import is_version_greater_or_equal
from great_expectations.compatibility.pyspark import functions as F
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.map_metric_provider import (
    MulticolumnMapMetricProvider,
)
from great_expectations.expectations.metrics.map_metric_provider.multicolumn_condition_partial import (  # noqa: E501 # FIXME CoP
    multicolumn_condition_partial,
)


class MulticolumnSumEqual(MulticolumnMapMetricProvider):
    condition_metric_name = "multicolumn_sum.equal"
    condition_domain_keys = (
        "batch_id",
        "table",
        "column_list",
        "row_condition",
        "condition_parser",
        "ignore_row_if",
    )
    condition_value_keys = ("sum_total",)

    @multicolumn_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_list, **kwargs):
        sum_total = kwargs.get("sum_total")
        row_wise_cond = column_list.sum(axis=1, skipna=False) == sum_total
        return row_wise_cond

    @multicolumn_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column_list, **kwargs):
        sum_total = kwargs.get("sum_total")
        # A sum with a NULL operand is NULL, and so are the comparison and its negation (the
        # unexpected condition), so the row would count as neither expected nor unexpected.
        # Requiring every column makes it FALSE instead, and the row is unexpected, as on pandas.
        row_wise_cond = sa.and_(
            sum(column_list) == sum_total, *[column.is_not(None) for column in column_list]
        )
        return row_wise_cond

    @multicolumn_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column_list, **kwargs):
        sum_total = kwargs.get("sum_total")
        if pyspark.pyspark and is_version_greater_or_equal(pyspark.pyspark.__version__, "4.0.0"):
            # Spark 4 evaluates arithmetic under ANSI semantics by default and raises
            # ARITHMETIC_OVERFLOW when an integral (Long) accumulator overflows, whereas
            # Spark 3 silently wraps. Widen only integral operands to an exact wide DECIMAL
            # so the row-wise sum cannot overflow. Decimal and floating operands are left
            # untouched so exact decimal equality (e.g. 0.1 + 0.2 == 0.3) is preserved and
            # floating sums keep their usual semantics (floats do not raise under ANSI).
            integral_types = (
                pyspark.types.ByteType,
                pyspark.types.ShortType,
                pyspark.types.IntegerType,
                pyspark.types.LongType,
            )
            operands = []
            for column_name in column_list.columns:
                coalesced = f"COALESCE(`{column_name}`, 0)"
                if isinstance(column_list.schema[column_name].dataType, integral_types):
                    operands.append(f"CAST({coalesced} AS DECIMAL(38, 0))")
                else:
                    operands.append(coalesced)
            expression = "+".join(operands)
            row_wise_cond = F.expr(expression) == F.lit(sum_total)
        else:
            # Spark 3 wraps integral overflow rather than raising, so keep the original
            # unwidened sum and literal comparison to preserve byte-identical results.
            expression = "+".join(
                [f"COALESCE({column_name}, 0)" for column_name in column_list.columns]
            )
            row_wise_cond = F.expr(expression) == F.lit(sum_total)
        return row_wise_cond
