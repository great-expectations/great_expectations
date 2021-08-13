from dateutil.parser import parse

from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnPairMapMetricProvider,
    column_pair_condition_partial,
)


class ColumnPairValuesAGreaterThanB(ColumnPairMapMetricProvider):
    condition_metric_name = "column_pair_values.a_greater_than_b"
    condition_value_keys = (
        "ignore_row_if",
        "or_equal",
        "parse_strings_as_datetimes",
        "allow_cross_type_comparisons",
    )
    condition_domain_keys = ("batch_id", "table", "column_A", "column_B")

    # TODO: <Alex>ALEX -- temporarily only a Pandas implementation is provided (others to follow).</Alex>
    # noinspection PyPep8Naming
    @column_pair_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_A, column_B, **kwargs):
        allow_cross_type_comparisons = kwargs.get("allow_cross_type_comparisons")
        if allow_cross_type_comparisons:
            raise NotImplementedError

        parse_strings_as_datetimes = kwargs.get("parse_strings_as_datetimes")
        if parse_strings_as_datetimes:
            # noinspection PyPep8Naming
            temp_column_A = column_A.map(parse)
            # noinspection PyPep8Naming
            temp_column_B = column_B.map(parse)
        else:
            temp_column_A = column_A
            temp_column_B = column_B

        or_equal = kwargs.get("or_equal")
        if or_equal:
            return temp_column_A >= temp_column_B
        else:
            return temp_column_A > temp_column_B
