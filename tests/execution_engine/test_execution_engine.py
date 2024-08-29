from __future__ import annotations

from typing import Dict, Tuple

import pandas as pd
import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.core.batch import BatchData, BatchMarkers
from great_expectations.core.metric_function_types import (
    MetricPartialFunctionTypeSuffixes,
    SummarizationMetricNameSuffixes,
)
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.expectations.row_conditions import (
    RowCondition,
    RowConditionParserType,
)

# Testing ordinary process of adding column row condition
from great_expectations.validator.computed_metric import MetricValue
from great_expectations.validator.metric_configuration import MetricConfiguration
from tests.expectations.test_util import get_table_columns_metric


@pytest.fixture
def test_execution_engine():
    """
    This fixture is for mocking the abstract ExecutionEngine class to test method functionality.
    Instead of using it's child classes in tests, which could override the parent methods,
    we create a subclass that implements abstract methods (raising exceptions if used)
    and use that fixture in tests.
    """

    class TestExecutionEngine(ExecutionEngine):
        def get_batch_data_and_markers(self, batch_spec) -> Tuple[BatchData, BatchMarkers]:  # type: ignore[explicit-override] # FIXME
            raise NotImplementedError

    return TestExecutionEngine()


@pytest.mark.unit
def test_add_column_row_condition_filter_null_row_condition_not_present(
    test_execution_engine,
):
    e = test_execution_engine

    # Checking that adding a simple column row condition is functional
    # default of add_column_row_condition is to apply filter_null=True
    domain_kwargs: dict = {}
    new_domain_kwargs = e.add_column_row_condition(domain_kwargs, "a")
    assert new_domain_kwargs == {
        "filter_conditions": [
            RowCondition(condition='col("a").notnull()', condition_type=RowConditionParserType.GE)
        ]
    }

    # Ensuring that this also works when formatted differently
    # default of add_column_row_condition is to apply filter_null=True
    new_domain_kwargs = e.add_column_row_condition({"column": "a"})
    assert new_domain_kwargs == {
        "column": "a",
        "filter_conditions": [
            RowCondition(condition='col("a").notnull()', condition_type=RowConditionParserType.GE)
        ],
    }


@pytest.mark.unit
def test_add_column_row_condition_filter_null_false_row_condition_not_present(
    test_execution_engine,
):
    e = test_execution_engine

    # Identity case
    # default of add_column_row_condition is to apply filter_null=True
    domain_kwargs: dict = {}
    new_domain_kwargs = e.add_column_row_condition(domain_kwargs, "a", filter_null=False)
    assert new_domain_kwargs == domain_kwargs


@pytest.mark.unit
def test_add_column_row_condition_filter_null_false_row_condition_present(
    test_execution_engine,
):
    e = test_execution_engine

    # Identity case
    # default of add_column_row_condition is to apply filter_null=True
    domain_kwargs: dict = {"row_condition": "some_condition"}
    new_domain_kwargs = e.add_column_row_condition(domain_kwargs, "a", filter_null=False)
    assert new_domain_kwargs == domain_kwargs


@pytest.mark.unit
def test_add_column_row_condition_filter_null_row_condition_present(
    test_execution_engine,
):
    e = test_execution_engine

    # Ensuring that we don't override if a row condition is present
    # default of add_column_row_condition is to apply filter_null=True
    domain_kwargs: dict = {"column": "a", "row_condition": "some_row_condition"}
    new_domain_kwargs = e.add_column_row_condition(domain_kwargs, filter_null=True)
    assert new_domain_kwargs == {
        "column": "a",
        "row_condition": "some_row_condition",
        "filter_conditions": [
            RowCondition(condition='col("a").notnull()', condition_type=RowConditionParserType.GE)
        ],
    }

    # Ensuring that we don't override if a row condition is present,
    # default of add_column_row_condition is to apply filter_null=True
    domain_kwargs: dict = {"column": "a", "row_condition": "some_row_condition"}
    new_domain_kwargs = e.add_column_row_condition(domain_kwargs)
    assert new_domain_kwargs == {
        "column": "a",
        "row_condition": "some_row_condition",
        "filter_conditions": [
            RowCondition(condition='col("a").notnull()', condition_type=RowConditionParserType.GE)
        ],
    }


@pytest.mark.unit
def test_add_column_row_condition_filter_null_row_condition_none(test_execution_engine):
    e = test_execution_engine

    # Ensuring that everything still works if a row condition of None given
    # default of add_column_row_condition is to apply filter_null=True
    domain_kwargs: dict = {"column": "a", "row_condition": None}
    new_domain_kwargs = e.add_column_row_condition(domain_kwargs)
    assert new_domain_kwargs == {
        "column": "a",
        "row_condition": None,
        "filter_conditions": [
            RowCondition(condition='col("a").notnull()', condition_type=RowConditionParserType.GE)
        ],
    }


# Edge cases
@pytest.mark.unit
def test_add_column_row_condition_with_unsupported_filter_nan_true(
    test_execution_engine,
):
    e = test_execution_engine

    # Ensuring that an attempt to filter nans within base class yields an error
    with pytest.raises(gx_exceptions.GreatExpectationsError) as error:
        _ = e.add_column_row_condition({}, "a", filter_nan=True)
    assert (
        "Base ExecutionEngine does not support adding nan condition filters" in error.value.message
    )


@pytest.mark.unit
def test_add_column_row_condition_with_unsupported_no_column_provided(
    test_execution_engine,
):
    e = test_execution_engine

    # Testing that error raised when column not given
    with pytest.raises(AssertionError):
        _ = e.add_column_row_condition({})


@pytest.mark.unit
def test_resolve_metrics_with_aggregates_and_column_map():
    # Testing resolve metric function for a variety of cases - test from test_core used
    df = pd.DataFrame({"a": [1, 2, 3, None]})
    engine = PandasExecutionEngine(batch_data_dict={"my_id": df})

    metrics: Dict[Tuple[str, str, str], MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[Tuple[str, str, str], MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)

    metrics.update(results)

    mean = MetricConfiguration(
        metric_name="column.mean",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    mean.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    stdev = MetricConfiguration(
        metric_name="column.standard_deviation",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    stdev.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    desired_metrics = (mean, stdev)
    results = engine.resolve_metrics(metrics_to_resolve=desired_metrics, metrics=metrics)
    metrics.update(results)

    desired_map_metric = MetricConfiguration(
        metric_name=f"column_values.z_score.{MetricPartialFunctionTypeSuffixes.MAP.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_map_metric.metric_dependencies = {
        "column.standard_deviation": stdev,
        "column.mean": mean,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_map_metric,), metrics=metrics)
    metrics.update(results)

    desired_threshold_condition_metric = MetricConfiguration(
        metric_name=f"column_values.z_score.under_threshold.{MetricPartialFunctionTypeSuffixes.CONDITION.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
    )
    desired_threshold_condition_metric.metric_dependencies = {
        f"column_values.z_score.{MetricPartialFunctionTypeSuffixes.MAP.value}": desired_map_metric,
        "table.columns": table_columns_metric,
    }
    results = engine.resolve_metrics(
        metrics_to_resolve=(desired_threshold_condition_metric,), metrics=metrics
    )
    metrics.update(results)
    assert list(results[desired_threshold_condition_metric.id][0]) == [
        False,
        False,
        False,
    ]

    desired_metric = MetricConfiguration(
        metric_name=f"column_values.z_score.under_threshold.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"double_sided": True, "threshold": 2},
    )
    desired_metric.metric_dependencies = {
        "unexpected_condition": desired_threshold_condition_metric,
    }
    results = engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics=metrics)
    metrics.update(results)
    assert results[desired_metric.id] == 0


@pytest.mark.unit
def test_resolve_metrics_with_extraneous_value_key():
    df = pd.DataFrame({"a": [1, 2, 3, None]})
    engine = PandasExecutionEngine(batch_data_dict={"my_id": df})

    metrics: Dict[Tuple[str, str, str], MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[Tuple[str, str, str], MetricValue]

    table_columns_metric, results = get_table_columns_metric(execution_engine=engine)

    metrics.update(results)

    mean = MetricConfiguration(
        metric_name="column.mean",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    mean.metric_dependencies = {
        "table.columns": table_columns_metric,
    }
    # Ensuring that an unused value key will not mess up computation
    stdev = MetricConfiguration(
        metric_name="column.standard_deviation",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs={"value_set": [1, 2, 3, 4, 5]},
    )
    stdev.metric_dependencies = {
        "table.columns": table_columns_metric,
    }

    desired_metrics = (mean, stdev)
    results = engine.resolve_metrics(metrics_to_resolve=desired_metrics, metrics=metrics)
    metrics.update(results)

    # Ensuring extraneous value key did not change computation
    assert metrics[("column.standard_deviation", "column=a", "value_set=[1, 2, 3, 4, 5]")] == 1.0


# Testing that metric resolution also works with metric partial function
@pytest.mark.unit
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
        metric_name=f"column_values.z_score.{MetricPartialFunctionTypeSuffixes.MAP.value}",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    desired_metric.metric_dependencies = {
        "column.standard_deviation": stdev,
        "column.mean": mean,
    }

    # Ensuring that incomplete metrics given raises a GreatExpectationsError
    with pytest.raises(gx_exceptions.GreatExpectationsError):
        engine.resolve_metrics(metrics_to_resolve=(desired_metric,), metrics={})
