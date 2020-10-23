# TODO
import numpy as np
import pandas as pd

import great_expectations.validator
from great_expectations import ExpectColumnMaxToBeBetween
from great_expectations.core import IDDict
from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.exceptions.metric_exceptions import (
    MetricError,
    MetricProviderError,
)
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.core.expect_column_value_z_scores_to_be_less_than import (
    ExpectColumnValueZScoresToBeLessThan,
)
from great_expectations.expectations.registry import get_expectation_impl
from great_expectations.validator import validator
from great_expectations.validator.validation_graph import (
    MetricEdge,
    MetricEdgeKey,
    ValidationGraph,
)
from great_expectations.validator.validator import Validator


# TODO: extraneous metric keys potentially as a result of recursive execution
def test_parse_validation_graph():
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, 6]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={"column": "a", "mostly": 0.9, "threshold": 4, "double_sided": True,},
    )
    expectation = ExpectColumnValueZScoresToBeLessThan(expectationConfiguration)
    batch = Batch(data=df)
    graph = ValidationGraph()
    for configuration in [expectationConfiguration]:
        expectation_impl = get_expectation_impl(
            "expect_column_value_z_scores_to_be_less_than"
        )
        validation_dependencies = expectation_impl(
            configuration
        ).get_validation_dependencies(configuration, PandasExecutionEngine,)

        for metric_name in validation_dependencies.get("metrics"):
            Validator()._populate_dependencies(
                graph, metric_name, configuration,
            )
    ready_metrics, needed_metrics = Validator()._parse_validation_graph(
        validation_graph=graph, metrics=validation_dependencies.get("metrics")
    )
    assert len(ready_metrics) == 4 and len(needed_metrics) == 5


# Should be passing tests even if given incorrect Metric data
def test_parse_validation_graph_with_bad_metrics_args():
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, 6]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={"column": "a", "mostly": 0.9, "threshold": 4, "double_sided": True,},
    )
    validator = Validator()
    graph = ValidationGraph()
    for configuration in [expectationConfiguration]:
        expectation_impl = get_expectation_impl(
            "expect_column_value_z_scores_to_be_less_than"
        )
        validation_dependencies = expectation_impl(
            configuration
        ).get_validation_dependencies(configuration, PandasExecutionEngine,)

        for metric_name in validation_dependencies.get("metrics"):
            validator._populate_dependencies(
                graph, metric_name, configuration,
            )
    ready_metrics, needed_metrics = validator._parse_validation_graph(
        validation_graph=graph, metrics=("nonexistent", "NONE")
    )
    assert len(ready_metrics) == 4 and len(needed_metrics) == 5


# Should be passing tests even if given bad metrics, but why is it registered as a ready metric when it's not???
def test_parse_validation_graph_with_nonmatching_validation_graph():
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, 6]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={"column": "a", "mostly": 0.9, "threshold": 4, "double_sided": True,},
    )
    validator = Validator()
    metric_error = None
    graph = ValidationGraph()
    for configuration in [expectationConfiguration]:
        expectation_impl = get_expectation_impl(
            "expect_column_value_z_scores_to_be_less_than"
        )
        validation_dependencies = expectation_impl(
            configuration
        ).get_validation_dependencies(configuration, PandasExecutionEngine,)

        for metric_name in validation_dependencies.get("metrics"):
            validator._populate_dependencies(
                graph, metric_name, configuration,
            )
        graph.add(
            MetricEdge(
                MetricEdgeKey(
                    metric_name="column.aggregate.max",
                    metric_domain_kwargs=IDDict(),
                    metric_value_kwargs=IDDict(),
                    filter_column_isnull=True,
                ),
                None,
            )
        )
    ready_metrics, needed_metrics = validator._parse_validation_graph(
        validation_graph=graph, metrics=validation_dependencies.get("metrics")
    )
    assert len(ready_metrics) == 5 and len(needed_metrics) == 5


# Talking-point: Isn't the sheer number of Metric Dependencies here a bit extraneous??
def test_populate_dependencies():
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, 6]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={"column": "a", "mostly": 0.9, "threshold": 4, "double_sided": True,},
    )
    expectation = ExpectColumnValueZScoresToBeLessThan(expectationConfiguration)
    batch = Batch(data=df)
    graph = ValidationGraph()
    for configuration in [expectationConfiguration]:
        expectation_impl = get_expectation_impl(
            "expect_column_value_z_scores_to_be_less_than"
        )
        validation_dependencies = expectation_impl(
            configuration
        ).get_validation_dependencies(configuration, PandasExecutionEngine,)

        for metric_name in validation_dependencies.get("metrics"):
            Validator()._populate_dependencies(
                graph, metric_name, configuration,
            )
    assert len(graph.edges) == 21


def test_populate_dependencies_with_incorrect_metric_name():
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, 6]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={"column": "a", "mostly": 0.9, "threshold": 4, "double_sided": True,},
    )
    expectation = ExpectColumnValueZScoresToBeLessThan(expectationConfiguration)
    batch = Batch(data=df)
    graph = ValidationGraph()
    for configuration in [expectationConfiguration]:
        expectation_impl = get_expectation_impl(
            "expect_column_value_z_scores_to_be_less_than"
        )
        validation_dependencies = expectation_impl(
            configuration
        ).get_validation_dependencies(configuration, PandasExecutionEngine,)

        try:
            Validator()._populate_dependencies(
                graph, "column_values.not_a_metric", configuration,
            )
        except MetricProviderError as e:
            graph = e

    assert isinstance(graph, MetricProviderError)


def test_graph_validate():
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, None]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={"column": "b", "mostly": 0.9, "threshold": 4, "double_sided": True,},
    )
    expectation = ExpectColumnValueZScoresToBeLessThan(expectationConfiguration)
    batch = Batch(data=df)
    result = Validator().graph_validate(
        batches={"batch_id": batch},
        configurations=[expectationConfiguration],
        execution_engine=PandasExecutionEngine(),
    )
    assert result == [
        ExpectationValidationResult(
            success=True,
            expectation_config=None,
            meta={},
            result={
                "element_count": 6,
                "unexpected_count": 0,
                "unexpected_percent": 0.0,
                "partial_unexpected_list": [],
                "missing_count": 1,
                "missing_percent": 16.666666666666664,
                "unexpected_percent_nonmissing": 0.0,
            },
            exception_info=None,
        )
    ]


# this might indicate that we need to validate configuration a little more strictly prior to actually validating
def test_graph_validate_with_bad_config():
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, None]})
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_max_to_be_between",
        kwargs={"column": "not_in_table", "min_value": 1, "max_value": 29},
    )
    expectation = ExpectColumnMaxToBeBetween(expectationConfiguration)
    batch = Batch(data=df)
    try:
        result = Validator().graph_validate(
            batches={"batch_id": batch},
            configurations=[expectationConfiguration],
            execution_engine=PandasExecutionEngine(),
        )
    except KeyError as e:
        result = e
    assert isinstance(result, KeyError)


# Tests that runtime configuration actually works during graph validation
def test_graph_validate_with_runtime_config():
    df = pd.DataFrame(
        {"a": [1, 5, 22, 3, 5, 10, 2, 3], "b": [97, 332, 3, 4, 5, 6, 7, None]}
    )
    expectationConfiguration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={"column": "b", "mostly": 1, "threshold": 2,},
    )
    expectation = ExpectColumnValueZScoresToBeLessThan(expectationConfiguration)
    batch = Batch(data=df)
    try:
        result = Validator().graph_validate(
            batches={"batch_id": batch},
            configurations=[expectationConfiguration],
            execution_engine=PandasExecutionEngine(),
            runtime_configuration={"result_format": "COMPLETE"},
        )
    except AssertionError as e:
        result = e
    assert result == [
        ExpectationValidationResult(
            success=False,
            meta={},
            result={
                "element_count": 8,
                "unexpected_count": 1,
                "unexpected_percent": 12.5,
                "partial_unexpected_list": [332.0],
                "missing_count": 1,
                "missing_percent": 12.5,
                "unexpected_percent_nonmissing": 14.285714285714285,
                "partial_unexpected_index_list": None,
                "partial_unexpected_counts": [{"value": 332.0, "count": 1}],
                "unexpected_list": [332.0],
                "unexpected_index_list": None,
            },
            expectation_config=None,
            exception_info=None,
        )
    ]
