from typing import Any, Dict, Tuple

import pandas as pd
from dateutil.parser import parse

from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.execution_engine.execution_engine import (
    MetricDomainTypes,
    MetricPartialFunctionTypes,
)
from great_expectations.expectations.metrics.map_metric import MapMetricProvider
from great_expectations.expectations.metrics.metric_provider import metric_partial
from great_expectations.expectations.metrics.util import filter_pair_metric_nulls


class MulticolumnSumEqual(MapMetricProvider):
    # TODO: <Alex>ALEX</Alex>
    condition_metric_name = "multicolumn_sum.equal"
    # metric_name = "multicolumn_sum.equal"
    # TODO: <Alex>ALEX</Alex>
    condition_value_keys = (
        # TODO: <Alex>ALEX</Alex>
        "sum_total",
        # TODO: <Alex>ALEX</Alex>
    )
    # TODO: <Alex>ALEX</Alex>
    domain_keys = ("batch_id", "table", "column_list")

    @metric_partial(
        engine=PandasExecutionEngine,
        partial_fn_type=MetricPartialFunctionTypes.MAP_CONDITION_SERIES,
        domain_type=MetricDomainTypes.MULTICOLUMN,
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
            metric_domain_kwargs, MetricDomainTypes.MULTICOLUMN
        )

        # TODO: <Alex>ALEX</Alex>
        row_wise_cond = df.sum(axis=1) == sum_total
        # TODO: <Alex>ALEX</Alex>
        # return row_wise_cond.all()
        return row_wise_cond, compute_domain_kwargs, accessor_domain_kwargs
        # TODO: <Alex>ALEX</Alex>
