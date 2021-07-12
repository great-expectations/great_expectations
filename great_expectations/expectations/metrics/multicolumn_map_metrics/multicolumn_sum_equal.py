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
        # "ignore_row_if",
        # "or_equal",
        # "parse_strings_as_datetimes",
        # "allow_cross_type_comparisons",
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
        print(f'\n[ALEX_TEST] METRIC_DOMAIN_KWARGS: {metric_domain_kwargs} ; TYPE: {str(type(metric_domain_kwargs))}')
        print(f'\n[ALEX_TEST] METRIC_VALUE_KWARGS: {metric_value_kwargs} ; TYPE: {str(type(metric_value_kwargs))}')
        print(f'\n[ALEX_TEST] DF: {df} ; TYPE: {str(type(df))}')
        print(f'\n[ALEX_TEST] COMPUTE_DOMAIN: {compute_domain_kwargs} ; TYPE: {str(type(compute_domain_kwargs))}')
        print(f'\n[ALEX_TEST] ACCESSOR_DOMAIN: {accessor_domain_kwargs} ; TYPE: {str(type(accessor_domain_kwargs))}')

        # TODO: <Alex>ALEX</Alex>
        # row_wise_cond: pd.Series = df.sum(axis=1) == sum_total
        # row_wise_cond: bool = df.sum(axis=1) == sum_total
        row_wise_cond = df.sum(axis=1) == sum_total
        # row_wise_cond: pd.core.series.Series = df.sum(axis=1) == sum_total
        # print(f'\n[ALEX_TEST] ROW_WISE_CONDITION.ALL(): {row_wise_cond.all()} ; TYPE: {str(type(row_wise_cond.all()))}')
        print(f'\n[ALEX_TEST] ROW_WISE_CONDITION: {row_wise_cond} ; TYPE: {str(type(row_wise_cond))}')
        # return df.sum(axis=1) == sum_total
        # TODO: <Alex>ALEX</Alex>
        # return row_wise_cond.all()
        return row_wise_cond, compute_domain_kwargs, accessor_domain_kwargs
        # TODO: <Alex>ALEX</Alex>
