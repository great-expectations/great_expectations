from typing import Tuple

import pandas as pd
import pytest

from great_expectations.core.batch import BatchMarkers
from great_expectations.exceptions import GreatExpectationsError
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.execution_engine.execution_engine import BatchData
from great_expectations.expectations.row_conditions import (
    RowCondition,
    RowConditionParserType,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

# Testing ordinary process of adding column row condition
from tests.expectations.test_util import get_table_columns_metric


@pytest.fixture
def test_execution_engine():
    """
    This fixture is for mocking the abstract ExecutionEngine class to test method functionality.
    Instead of using it's child classes in tests, which could override the parent mehods,
    we create a subclass that implements abstract methods and use it in tests.
    """

    class TestExecutionEngine(ExecutionEngine):
        def get_batch_data_and_markers(
            self, batch_spec
        ) -> Tuple[BatchData, BatchMarkers]:
            raise NotImplementedError

    return TestExecutionEngine()


def test_add_column_row_condition(test_execution_engine):
    e = test_execution_engine

    # Checking that adding a simple column row condition is functional
    new_domain_kwargs = e.add_column_row_condition({}, "a")
    assert new_domain_kwargs == {
        "filter_conditions": [
            RowCondition(
                condition='col("a").notnull()', condition_type=RowConditionParserType.GE
            )
        ]
    }

    # Ensuring that this also works when formatted differently
    new_domain_kwargs = e.add_column_row_condition({"column": "a"})
    assert new_domain_kwargs == {
        "column": "a",
        "filter_conditions": [
            RowCondition(
                condition='col("a").notnull()', condition_type=RowConditionParserType.GE
            )
        ],
    }

    # Ensuring that everything still works if a row condition of None given
    new_domain_kwargs = e.add_column_row_condition(
        {"column": "a", "row_condition": None}
    )
    assert new_domain_kwargs == {
        "column": "a",
        "row_condition": None,
        "filter_conditions": [
            RowCondition(
                condition='col("a").notnull()', condition_type=RowConditionParserType.GE
            )
        ],
    }

    # Identity case
    new_domain_kwargs = e.add_column_row_condition({}, "a", filter_null=False)
    assert new_domain_kwargs == {}


def test_add_column_row_condition_filter_null_pandas(test_execution_engine):
    e = test_execution_engine

    # Ensuring that we don't override if a row condition is present
    new_domain_kwargs = e.add_column_row_condition(
        {"column": "a", "row_condition": "some_row_condition"}, filter_null=True
    )
    assert new_domain_kwargs == {
        "column": "a",
        "row_condition": "some_row_condition",
        "filter_conditions": [
            RowCondition(
                condition='col("a").notnull()', condition_type=RowConditionParserType.GE
            )
        ],
    }

    # Ensuring that we don't override if a row condition is present,
    # with default filter_null value
    new_domain_kwargs = e.add_column_row_condition(
        {"column": "a", "row_condition": "some_row_condition"},
    )
    assert new_domain_kwargs == {
        "column": "a",
        "row_condition": "some_row_condition",
        "filter_conditions": [
            RowCondition(
                condition='col("a").notnull()', condition_type=RowConditionParserType.GE
            )
        ],
    }


# Edge cases
def test_add_column_row_condition_with_unsupported_conditions(test_execution_engine):
    e = test_execution_engine

    # Ensuring that an attempt to filter nans within base class yields an error
    with pytest.raises(GreatExpectationsError) as error:
        new_domain_kwargs = e.add_column_row_condition({}, "a", filter_nan=True)

    # Testing that error raised when column not given
    with pytest.raises(AssertionError) as error:
        new_domain_kwargs = e.add_column_row_condition({})


def test_resolve_metrics_with_aggregates_and_column_map():
    # Testing resolve metric function for a variety of cases - test from test_core used
    df = pd.DataFrame({"a": [1, 2, 3, None]})
    engine = PandasExecutionEngine(batch_data_dict={"my_id": df})

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)

    metrics.update(results)

    mean = MetricConfiguration(
        metric_name="column.mean",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    stdev = MetricConfiguration(
        metric_name="column.standard_deviation",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    desired_metrics = (mean, stdev)
    results = engine.resolve_metrics(
        metrics_to_resolve=desired_metrics, metrics=metrics
    )
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column_values.z_score.map",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
        metric_dependencies={
            "column.standard_deviation": stdev,
            "column.mean": mean,
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)

    desired_metric = MetricConfiguration(
        metric_name="column_values.z_score.under_threshold.condition",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
        metric_dependencies={
            "column_values.z_score.map": desired_metric,
            "table.columns": table_columns_metric,
        },
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
    assert list(results[desired_metric.id][0]) == [False, False, False]

    desired_metric = MetricConfiguration(
        metric_name="column_values.z_score.under_threshold.unexpected_count",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
        metric_dependencies={"unexpected_condition": desired_metric},
    )
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_metric,), metrics=metrics
    )
    metrics.update(results)
    assert results[desired_metric.id] == 0


def test_resolve_metrics_with_extraneous_value_key():
    df = pd.DataFrame({"a": [1, 2, 3, None]})
    engine = PandasExecutionEngine(batch_data_dict={"my_id": df})

    metrics: dict = {}

    table_columns_metric: MetricConfiguration
    results: dict

    table_columns_metric, results = get_table_columns_metric(engine=engine)

    metrics.update(results)

    mean = MetricConfiguration(
        metric_name="column.mean",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )
    # Ensuring that an unused value key will not mess up computation
    stdev = MetricConfiguration(
        metric_name="column.standard_deviation",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3, 4, 5]},
        metric_dependencies={
            "table.columns": table_columns_metric,
        },
    )

    desired_metrics = (mean, stdev)
    results = engine.resolve_metrics(
        metrics_to_resolve=desired_metrics, metrics=metrics
    )
    metrics.update(results)

    # Ensuring extraneous value key did not change computation
    assert (
        metrics[("column.standard_deviation", "column=a", "value_set=[1, 2, 3, 4, 5]")]
        == 1.0
    )


# Testing that metric resolution also works with metric partial function
def test_resolve_metrics_with_incomplete_metric_input():
    engine = PandasExecutionEngine()

    mean = MetricConfiguration(
        metric_name="column.mean",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    stdev = MetricConfiguration(
        metric_name="column.standard_deviation",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )

    desired_metric = MetricConfiguration(
        metric_name="column_values.z_score.map",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
        metric_dependencies={
            "column.standard_deviation": stdev,
            "column.mean": mean,
        },
    )

    # Ensuring that incomplete metrics given raises a GreatExpectationsError
    with pytest.raises(GreatExpectationsError) as error:
        engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics={})
