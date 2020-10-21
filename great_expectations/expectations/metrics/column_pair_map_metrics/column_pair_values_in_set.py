from typing import Any, Dict, Tuple

import numpy as np
import pandas as pd
from dateutil.parser import parse

from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_map_metric import (
    ColumnMapMetric,
    column_map_condition,
    map_condition,
)
from great_expectations.expectations.metrics.metric import metric
from great_expectations.expectations.metrics.utils import filter_pair_metric_nulls


class ColumnPairValuesInSet(ColumnMapMetric):
    condition_metric_name = "column_pair_values.in_set"
    condition_value_keys = (
        "value_pairs_set",
        "ignore_row_if",
    )
    domain_keys = ("batch_id", "table", "column_a", "column_b")

    @map_condition(engine=PandasExecutionEngine, bundle_metric=False)
    def _pandas(
        cls,
        execution_engine: "PandasExecutionEngine",
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[Tuple, Any],
        runtime_configuration: dict,
    ):

        value_pairs_set = metric_value_kwargs.get("value_pairs_set")
        ignore_row_if = metric_value_kwargs.get("ignore_row_if")
        if not ignore_row_if:
            ignore_row_if = "both_values_are_missing"

        df, compute_domain, _ = execution_engine.get_compute_domain(
            metric_domain_kwargs
        )

        column_A, column_B = filter_pair_metric_nulls(
            df[metric_domain_kwargs["column_a"]],
            df[metric_domain_kwargs["column_b"]],
            ignore_row_if=ignore_row_if,
        )

        if value_pairs_set is None:
            # vacuously true
            return np.ones(len(column_A), dtype=np.bool_)

        temp_df = pd.DataFrame({"A": column_A, "B": column_B})
        value_pairs_set = {(x, y) for x, y in value_pairs_set}

        results = []
        for i, t in temp_df.iterrows():
            if pd.isnull(t["A"]):
                a = None
            else:
                a = t["A"]

            if pd.isnull(t["B"]):
                b = None
            else:
                b = t["B"]

            results.append((a, b) in value_pairs_set)

        return pd.Series(results)
