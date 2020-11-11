import copy
from typing import Any, Dict, Tuple

from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import (
    MetricDomainTypes,
    MetricPartialFunctionTypes,
)
from great_expectations.expectations.metrics.import_manager import F
from great_expectations.expectations.metrics.map_metric import MapMetricProvider
from great_expectations.expectations.metrics.metric_provider import metric_partial
from great_expectations.expectations.metrics.util import filter_pair_metric_nulls


class ColumnPairValuesEqual(MapMetricProvider):
    condition_metric_name = "column_pair_values.equal"
    condition_value_keys = ("ignore_row_if",)
    domain_keys = ("batch_id", "table", "column_A", "column_B")
    default_kwarg_values = {"ignore_row_if": "both_values_are_missing"}

    @metric_partial(
        engine=PandasExecutionEngine,
        partial_fn_type=MetricPartialFunctionTypes.MAP_CONDITION_SERIES,
        domain_type=MetricDomainTypes.COLUMN_PAIR,
    )
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
        (
            df,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            metric_domain_kwargs, MetricDomainTypes.COLUMN_PAIR
        )

        column_A, column_B = filter_pair_metric_nulls(
            df[metric_domain_kwargs["column_A"]],
            df[metric_domain_kwargs["column_B"]],
            ignore_row_if=ignore_row_if,
        )

        return column_A == column_B, compute_domain_kwargs, accessor_domain_kwargs

    @metric_partial(
        engine=SparkDFExecutionEngine,
        partial_fn_type=MetricPartialFunctionTypes.MAP_CONDITION_FN,
        domain_type=MetricDomainTypes.COLUMN_PAIR,
    )
    def _spark(
        cls,
        execution_engine: "SparkDFExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        ignore_row_if = metric_value_kwargs["ignore_row_if"]
        compute_domain_kwargs = copy.deepcopy(metric_domain_kwargs)

        if ignore_row_if == "both_values_are_missing":
            compute_domain_kwargs["row_condition"] = (
                F.col(metric_domain_kwargs["column_A"]).isNotNull()
                & F.col(metric_domain_kwargs["column_B"]).isNotNull()
            )
            compute_domain_kwargs["condition_parser"] = "spark"
        elif ignore_row_if == "either_value_is_missing":
            compute_domain_kwargs["row_condition"] = (
                F.col(metric_domain_kwargs["column_A"]).isNotNull()
                | F.col(metric_domain_kwargs["column_B"]).isNotNull()
            )
            compute_domain_kwargs["condition_parser"] = "spark"

        (
            df,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            compute_domain_kwargs, MetricDomainTypes.COLUMN_PAIR
        )

        return (
            df[metric_domain_kwargs["column_A"]]
            == df[metric_domain_kwargs["column_B"]],
            compute_domain_kwargs,
            accessor_domain_kwargs,
        )
