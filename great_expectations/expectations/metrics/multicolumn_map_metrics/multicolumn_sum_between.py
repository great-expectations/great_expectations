from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.expectations.metrics.map_metric_provider import (
    MulticolumnMapMetricProvider,
    multicolumn_condition_partial,
)


class MulticolumnSumBetween(MulticolumnMapMetricProvider):
    condition_metric_name = "multicolumn_sum.between"
    condition_domain_keys = (
        "batch_id",
        "table",
        "column_list",
        "row_condition",
        "condition_parser",
        "ignore_row_if",
    )
    condition_value_keys = (
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
    )

    @multicolumn_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_list, **kwargs):
        min_value = kwargs.get("min_value")
        max_value = kwargs.get("max_value")
        strict_min = kwargs.get("strict_min")
        strict_max = kwargs.get("strict_max")
        
        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        series = column_list.sum(axis=1, skipna=False)

        if min_value is None:
            if strict_max:
                return series < max_value
            else:
                return series <= max_value

        elif max_value is None:
            if strict_min:
                return min_value < series
            else:
                return min_value <= series
            
        else:
            if strict_min and strict_max:
                return (min_value < series) & (series < max_value)
            elif strict_min:
                return (min_value < series) & (series <= max_value)
            elif strict_max:
                return (min_value <= series) & (series < max_value)
            else:
                return (min_value <= series) & (series <= max_value)

    @multicolumn_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column_list, **kwargs):
        min_value = kwargs.get("min_value")
        max_value = kwargs.get("max_value")
        strict_min = kwargs.get("strict_min")
        strict_max = kwargs.get("strict_max")
        
        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")
        
        row_wise_sum = sum(column_list)

        if min_value is None:
            if strict_max:
                return row_wise_sum < max_value
            else:
                return row_wise_sum <= max_value

        elif max_value is None:
            if strict_min:
                return min_value < row_wise_sum
            else:
                return min_value <= row_wise_sum

        else:
            if strict_min and strict_max:
                return sa.and_(min_value < row_wise_sum, row_wise_sum < max_value)
            elif strict_min:
                return sa.and_(min_value < row_wise_sum, row_wise_sum <= max_value)
            elif strict_max:
                return sa.and_(min_value <= row_wise_sum, row_wise_sum < max_value)
            else:
                return sa.and_(min_value <= row_wise_sum, row_wise_sum <= max_value)

    @multicolumn_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column_list, **kwargs):
        min_value = kwargs.get("min_value")
        max_value = kwargs.get("max_value")
        strict_min = kwargs.get("strict_min")
        strict_max = kwargs.get("strict_max")

        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")
        
        expression = "+".join(
            [f"COALESCE({column_name}, 0)" for column_name in column_list.columns]
        )
        row_wise_sum = F.expr(expression)

        if min_value is None:
            if strict_max:
                return row_wise_sum < max_value
            else:
                return row_wise_sum <= max_value

        elif max_value is None:
            if strict_min:
                return min_value < row_wise_sum
            else:
                return min_value <= row_wise_sum

        else:
            if strict_min and strict_max:
                return (min_value < row_wise_sum) & (row_wise_sum < max_value)
            elif strict_min:
                return (min_value < row_wise_sum) & (row_wise_sum <= max_value)
            elif strict_max:
                return (min_value <= row_wise_sum) & (row_wise_sum < max_value)
            else:
                return (min_value <= row_wise_sum) & (row_wise_sum <= max_value)
