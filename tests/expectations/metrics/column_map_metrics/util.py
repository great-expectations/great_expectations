from typing import Dict, Tuple

from great_expectations.core.metric_function_types import (
    MetricPartialFunctionTypeSuffixes,
)
from great_expectations.validator.computed_metric import MetricValue
from great_expectations.validator.metric_configuration import MetricConfiguration
from tests.expectations.test_util import get_table_columns_metric


def _build_table_columns_and_unexpected(
    engine, metric_value_kwargs
) -> Tuple[MetricConfiguration, MetricConfiguration, dict]:
    """
    Helper method build the dependencies needed for unexpected_indices (ID/PK) related metrics.

    The unexpected_indices related metrics are dependent on :
        - unexpected_condition
        - table_columns

    This method takes in the engine (connected to data), and metric value kwargs dictionary to build the metrics
    and return as a tuple


    Args:
        engine (ExecutionEngine): has connection to data
        metric_value_kwargs (dict): can be

    Returns:

        Tuple with MetricConfigurations corresponding to unexpected_condition and table_columns metric, as well as metrics dict.

    """
    metrics: Dict[Tuple[str, str, str], MetricValue] = {}

    # get table_columns_metric
    table_columns_metric: MetricConfiguration
    results: Dict[Tuple[str, str, str], MetricValue]
    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)
    metrics.update(results)

    # unexpected_condition metric
    unexpected_condition_metric: MetricConfiguration = MetricConfiguration(
        metric_name=f"column_values.in_set.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "animals"},
        metric_value_kwargs=metric_value_kwargs,
    )
    unexpected_condition_metric.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    metrics = engine.resolve_metrics(
        metrics_to_resolve=(unexpected_condition_metric,), metrics=metrics
    )

    return table_columns_metric, unexpected_condition_metric, metrics
