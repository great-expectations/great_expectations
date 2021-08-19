from dateutil.parser import parse

from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.import_manager import sa
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
    condition_value_keys = (
        "or_equal",
        "parse_strings_as_datetimes",
        "allow_cross_type_comparisons",
    )

    # TODO: <Alex>ALEX -- temporarily only Pandas and SQL Alchemy implementations are provided (Spark to follow).</Alex>
    # noinspection PyPep8Naming
    @column_pair_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column_A, column_B, **kwargs):
        allow_cross_type_comparisons = kwargs.get("allow_cross_type_comparisons")
        if allow_cross_type_comparisons:
            raise NotImplementedError

        parse_strings_as_datetimes = kwargs.get("parse_strings_as_datetimes")
        if parse_strings_as_datetimes:
            temp_column_A = column_A.map(parse)
            temp_column_B = column_B.map(parse)
        else:
            temp_column_A = column_A
            temp_column_B = column_B

        or_equal = kwargs.get("or_equal")
        if or_equal:
            return temp_column_A >= temp_column_B
        else:
            return temp_column_A > temp_column_B

    # noinspection PyPep8Naming
    @column_pair_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column_A, column_B, **kwargs):
        allow_cross_type_comparisons = kwargs.get("allow_cross_type_comparisons")
        if allow_cross_type_comparisons:
            raise NotImplementedError

        parse_strings_as_datetimes = kwargs.get("parse_strings_as_datetimes")
        if parse_strings_as_datetimes:
            raise NotImplementedError

        or_equal = kwargs.get("or_equal")
        if or_equal:
            return sa.case((column_A >= column_B, True), else_=False)
        else:
            return sa.case((column_A > column_B, True), else_=False)
