from typing import Any, Dict, Tuple

from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.execution_engine.execution_engine import (
    MetricDomainTypes,
    MetricPartialFunctionTypes,
)
from great_expectations.expectations.metrics.map_metric import MapMetricProvider
from great_expectations.expectations.metrics.metric_provider import metric_partial


class MulticolumnSumEqual(MapMetricProvider):
    condition_metric_name = "multicolumn_sum.equal"
    condition_value_keys = ("sum_total",)
    condition_domain_keys = ("batch_id", "table", "column_list")

    # TODO: <Alex>ALEX -- temporarily only a Pandas implementation is provided (for investigation purposes).</Alex>
    @metric_partial(
        engine=PandasExecutionEngine,
        partial_fn_type=MetricPartialFunctionTypes.MAP_CONDITION_SERIES,
        domain_type=MetricDomainTypes.MULTICOLUMN,
        # domain_type=MetricDomainTypes.IDENTITY,  # In which situations should this "domain_type" be used?
    )
    def _pandas(
        cls,
        execution_engine: "PandasExecutionEngine",
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: Dict,
    ):
        sum_total = metric_value_kwargs.get("sum_total")
        (
            df,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            metric_domain_kwargs,
            MetricDomainTypes.MULTICOLUMN
            # MetricDomainTypes.IDENTITY  # In which situations should this "domain_type" be used?
        )

        # Is logic based on specific "accessor_domain_kwargs" keys a proper coding pattern in metric implementations?
        if "column_list" in accessor_domain_kwargs:
            df = df[accessor_domain_kwargs["column_list"]]

        # The fact that this condition metric must implement the negative logic in order to obtain the correct result
        # suggests the need for another condition style metric provider (e.g., "MulticolumnMapMetricProvider").
        row_wise_cond = ~(df.sum(axis=1) == sum_total)

        return row_wise_cond, compute_domain_kwargs, accessor_domain_kwargs
