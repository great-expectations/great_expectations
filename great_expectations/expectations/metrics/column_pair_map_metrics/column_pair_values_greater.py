from typing import Any, Dict, Tuple

from dateutil.parser import parse

from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_map_metric import (
    MapMetricProvider,
    column_map_condition,
    map_condition,
)
from great_expectations.expectations.metrics.metric_provider import metric
from great_expectations.expectations.metrics.util import filter_pair_metric_nulls


class ColumnPairValuesAGreaterThanB(MapMetricProvider):
    condition_metric_name = "column_pair_values.a_greater_than_b"
    condition_value_keys = (
        "ignore_row_if",
        "or_equal",
        "parse_strings_as_datetimes",
        "allow_cross_type_comparisons",
    )
    domain_keys = ("batch_id", "table", "column_a", "column_b")

    @map_condition(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        execution_engine: "PandasExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):

        ignore_row_if = metric_value_kwargs.get("ignore_row_if")
        if not ignore_row_if:
            ignore_row_if = "both_values_are_missing"
        or_equal = metric_value_kwargs.get("or_equal")
        parse_strings_as_datetimes = metric_value_kwargs.get(
            "parse_strings_as_datetimes"
        )
        allow_cross_type_comparisons = metric_value_kwargs.get(
            "allow_cross_type_comparisons"
        )

        df, compute_domain, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs
        )

        column_A, column_B = filter_pair_metric_nulls(
            df[metric_domain_kwargs["column_a"]],
            df[metric_domain_kwargs["column_b"]],
            ignore_row_if=ignore_row_if,
        )

        if allow_cross_type_comparisons:
            raise NotImplementedError

        if parse_strings_as_datetimes:
            temp_column_A = column_A.map(parse)
            temp_column_B = column_B.map(parse)

        else:
            temp_column_A = column_A
            temp_column_B = column_B

        if or_equal:
            return temp_column_A >= temp_column_B
        else:
            return temp_column_A > temp_column_B
