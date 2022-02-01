import json 
import dataprofiler as dp


from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
)

from great_expectations.expectations.expectation import (
    ColumnExpectation,
    Expectation,
    ExpectationConfiguration,
    InvalidExpectationConfigurationError,
    _format_map_output,
)
from great_expectations.expectations.metrics.column_aggregate_metric import (
    ColumnMetricProvider,
    column_aggregate_value,
)


class ColumnValuesProbabilisticallyMatchDataLabel(ColumnMetricProvider):
    """MetricProvider Class for Data Label Probability Matching Threshold"""
    pass


class ExpectColumnValuesToProbabilisticallyMatchDataLabel(ColumnExpectation):
    """
    This function builds upon the custom column map expectations of Great Expectations. This function asks the question a yes/no question of each row in the user-specified column; namely, does the confidence threshold provided by the DataProfiler model exceed the user-specified threshold.

    :param: column,  
    :param: data_label,
    :param: threshold

    df.expect_column_values_to_probabilistically_match_data_label(
        column,
        data_label=<>,
        threshold=float(0<=1)
    )
    """
    