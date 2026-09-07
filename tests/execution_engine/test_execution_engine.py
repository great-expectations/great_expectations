from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Tuple
from unittest.mock import patch

import pandas as pd
import pytest

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.metric_function_types import (
    MetricPartialFunctionTypeSuffixes,
    SummarizationMetricNameSuffixes,
)
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.execution_engine.execution_engine import MetricComputationConfiguration
from great_expectations.expectations.legacy_row_conditions import (
    RowCondition,
    RowConditionParserType,
)

# Testing ordinary process of adding column row condition
from great_expectations.validator.metric_configuration import (
    MetricConfiguration,
    MetricConfigurationID,
)
from tests.expectations.test_util import get_table_columns_metric

if TYPE_CHECKING:
    from great_expectations.core.batch import BatchData, BatchMarkers
    from great_expectations.expectations.row_conditions import (
        AndCondition,
        ComparisonCondition,
        NullityCondition,
        OrCondition,
    )
    from great_expectations.validator.computed_metric import MetricValue


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

        @override
        def _comparison_condition_to_filter_clause(self, condition: ComparisonCondition) -> str:
            raise NotImplementedError

        @override
        def _nullity_condition_to_filter_clause(self, condition: NullityCondition) -> str:
            raise NotImplementedError

        @override
        def _and_condition_to_filter_clause(self, condition: AndCondition) -> str:
            raise NotImplementedError

        @override
        def _or_condition_to_filter_clause(self, condition: OrCondition) -> str:
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

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

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

    metrics: Dict[MetricConfigurationID, MetricValue] = {}

    table_columns_metric: MetricConfiguration
    results: Dict[MetricConfigurationID, MetricValue]

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
    assert (
        metrics[
            MetricConfigurationID(
                "column.standard_deviation", "column=a", "value_set=[1, 2, 3, 4, 5]"
            )
        ]
        == 1.0
    )


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


@pytest.mark.unit
def test_bundle_failure_falls_back_to_individual_metric_computation(
    test_execution_engine,
):
    """When a bulk metric query fails, each metric should be computed individually.

    Metrics that succeed individually should be returned; only metrics that also
    fail individually should appear in the MetricResolutionError.failed_metrics.
    This ensures one bad metric cannot poison unrelated metrics in the same bundle.
    """
    engine = test_execution_engine

    metric_a = MetricConfiguration(
        metric_name="column.mean",
        metric_domain_kwargs={"column": "a"},
        metric_value_kwargs=None,
    )
    metric_b = MetricConfiguration(
        metric_name="column.mean",
        metric_domain_kwargs={"column": "b"},
        metric_value_kwargs=None,
    )

    config_a = MetricComputationConfiguration(
        metric_configuration=metric_a,
        metric_fn=lambda: None,  # unused — resolve_metric_bundle is mocked
        metric_provider_kwargs={},
    )
    config_b = MetricComputationConfiguration(
        metric_configuration=metric_b,
        metric_fn=lambda: None,  # unused — resolve_metric_bundle is mocked
        metric_provider_kwargs={},
    )

    individual_error = Exception("column b is not numeric")

    call_count = 0

    def mock_resolve_bundle(metric_fn_bundle):
        nonlocal call_count
        call_count += 1
        configs = list(metric_fn_bundle)
        if call_count == 1:
            # First call is the bulk query — fail to trigger fallback
            raise RuntimeError("bulk query failed")
        # Individual fallback calls: succeed for metric_a, fail for metric_b
        assert len(configs) == 1
        if configs[0].metric_configuration == metric_a:
            return {metric_a.id: 2.0}
        raise individual_error

    with patch.object(engine, "resolve_metric_bundle", side_effect=mock_resolve_bundle):
        with pytest.raises(gx_exceptions.MetricResolutionError) as exc_info:
            engine._process_direct_and_bundled_metric_computation_configurations(
                metric_fn_direct_configurations=[],
                metric_fn_bundle_configurations=[config_a, config_b],
            )

    err = exc_info.value
    # Only metric_b should be reported as failed
    assert list(err.failed_metrics) == [metric_b]
    # resolve_metric_bundle was called once for the bulk attempt and once per metric
    assert call_count == 3
