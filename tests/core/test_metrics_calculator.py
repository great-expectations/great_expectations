import datetime
from typing import Union

import pandas as pd
import pytest

from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.self_check.util import get_test_validator_with_data
from great_expectations.util import isclose
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.metrics_calculator import MetricsCalculator
from great_expectations.validator.validator import Validator


@pytest.fixture
def integer_and_datetime_sample_dataset() -> dict:
    week_idx: int
    return {
        "a": [
            0,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
        ],
        "b": [
            datetime.datetime(2021, 1, 1, 0, 0, 0)
            + datetime.timedelta(days=(week_idx * 7))
            for week_idx in range(12)
        ],
    }


# noinspection PyUnusedLocal
@pytest.mark.parametrize(
    "backend,",
    [
        pytest.param(
            "pandas",
        ),
        pytest.param(
            "sqlite",
        ),
        pytest.param(
            "spark",
        ),
    ],
)
def test_column_partition_metric(
    sa, spark_session, integer_and_datetime_sample_dataset: dict, backend: str
):
    """
    Test of "column.partition" metric for both, standard numeric column and "datetime.datetime" valued column.

    The "column.partition" metric depends on "column.max" metric and on "column.max" metric.

    For standard numerical data, test set contains 12 evenly spaced integers.
    For "datetime.datetime" data, test set contains 12 dates, starting with January 1, 2021, separated by 7 days.

    Expected partion boundaries are pre-computed algorithmically and asserted to be "close" to actual metric values.
    """
    validator_with_data: Validator = get_test_validator_with_data(
        execution_engine=backend,
        data=integer_and_datetime_sample_dataset,
    )

    metrics_calculator: MetricsCalculator = validator_with_data.metrics_calculator

    seconds_in_week = 604800

    n_bins = 10

    increment: Union[float, datetime.timedelta]
    idx: int
    element: Union[float, pd.Timestamp]

    desired_metric = MetricConfiguration(
        metric_name="column.partition",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={
            "bins": "uniform",
            "n_bins": n_bins,
            "allow_relative_error": False,
        },
        metric_dependencies=None,
    )
    results = metrics_calculator.compute_metrics(metric_configurations=[desired_metric])

    increment = float(n_bins + 1) / n_bins
    assert all(
        isclose(operand_a=element, operand_b=(increment * idx))
        for idx, element in enumerate(results[desired_metric.id])
    )

    # Test using "datetime.datetime" column.

    desired_metric = MetricConfiguration(
        metric_name="column.partition",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs={
            "bins": "uniform",
            "n_bins": n_bins,
            "allow_relative_error": False,
        },
        metric_dependencies=None,
    )
    results = metrics_calculator.compute_metrics(metric_configurations=[desired_metric])

    increment = datetime.timedelta(
        seconds=(seconds_in_week * float(n_bins + 1) / n_bins)
    )
    assert all(
        isclose(
            operand_a=element.to_pydatetime()
            if isinstance(validator_with_data.execution_engine, PandasExecutionEngine)
            else element,
            operand_b=(datetime.datetime(2021, 1, 1, 0, 0, 0) + (increment * idx)),
        )
        for idx, element in enumerate(results[desired_metric.id])
    )
