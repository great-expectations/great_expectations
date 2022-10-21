from typing import Any, Dict, Optional, Set, Tuple, Union, cast
from unittest import mock

import pandas as pd
import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.core import IDDict
from great_expectations.core.batch import Batch, RuntimeBatchRequest
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.expectations.registry import get_expectation_impl
from great_expectations.validator.exception_info import ExceptionInfo
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validation_graph import (
    MAX_METRIC_COMPUTATION_RETRIES,
    ExpectationValidationGraph,
    MetricEdge,
    ValidationGraph,
)
from great_expectations.validator.validator import Validator


@pytest.fixture
def metric_edge(
    table_head_metric_config: MetricConfiguration,
    column_histogram_metric_config: MetricConfiguration,
) -> MetricEdge:
    return MetricEdge(
        left=table_head_metric_config, right=column_histogram_metric_config
    )


@pytest.fixture
def validation_graph_with_single_edge(metric_edge: MetricEdge) -> ValidationGraph:
    edges = [metric_edge]
    return ValidationGraph(edges=edges)


@pytest.fixture
def expect_column_values_to_be_unique_expectation_config() -> ExpectationConfiguration:
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        meta={},
        kwargs={"column": "provider_id", "result_format": "BASIC"},
    )


@pytest.fixture
def expectation_validation_graph(
    expect_column_values_to_be_unique_expectation_config: ExpectationConfiguration,
) -> ExpectationValidationGraph:
    return ExpectationValidationGraph(
        configuration=expect_column_values_to_be_unique_expectation_config
    )


@pytest.mark.parametrize(
    "left_fixture_name,right_fixture_name,id",
    [
        pytest.param(
            "table_head_metric_config",
            None,
            (
                (
                    "table.head",
                    "batch_id=abc123",
                    "n_rows=5",
                ),
                None,
            ),
        ),
        pytest.param(
            "table_head_metric_config",
            "column_histogram_metric_config",
            (
                (
                    "table.head",
                    "batch_id=abc123",
                    "n_rows=5",
                ),
                (
                    "column.histogram",
                    "batch_id=def456",
                    "bins=5",
                ),
            ),
        ),
    ],
)
@pytest.mark.unit
def test_MetricEdge_init(
    left_fixture_name: str,
    right_fixture_name: Optional[str],
    id: tuple,
    request,
) -> None:
    left: MetricConfiguration = request.getfixturevalue(left_fixture_name)
    right: Optional[MetricConfiguration] = None
    if right_fixture_name:
        right = request.getfixturevalue(right_fixture_name)

    edge = MetricEdge(left=left, right=right)

    assert edge.left == left
    assert edge.right == right
    assert edge.id == id


@pytest.mark.unit
def test_ValidationGraph_init_no_input_edges() -> None:
    graph = ValidationGraph()

    assert graph.edges == []
    assert graph.edge_ids == set()


@pytest.mark.unit
def test_ValidationGraph_init_with_input_edges(
    metric_edge: MetricEdge,
) -> None:
    edges = [metric_edge]
    graph = ValidationGraph(edges=edges)

    assert graph.edges == edges
    assert graph.edge_ids == {e.id for e in edges}


@pytest.mark.unit
def test_ValidationGraph_add(metric_edge: MetricEdge) -> None:
    graph = ValidationGraph()

    assert graph.edges == []
    assert graph.edge_ids == set()

    graph.add(edge=metric_edge)

    assert graph.edges == [metric_edge]
    assert metric_edge.id in graph.edge_ids


@pytest.mark.unit
def test_ExpectationValidationGraph_constructor(
    expect_column_values_to_be_unique_expectation_config: ExpectationConfiguration,
    expectation_validation_graph: ExpectationValidationGraph,
) -> None:
    assert (
        expectation_validation_graph.configuration
        == expect_column_values_to_be_unique_expectation_config
    )
    assert expectation_validation_graph.graph.__dict__ == ValidationGraph().__dict__


@pytest.mark.unit
def test_ExpectationValidationGraph_update(
    expectation_validation_graph: ExpectationValidationGraph,
    validation_graph_with_single_edge: ValidationGraph,
) -> None:
    assert len(expectation_validation_graph.graph.edges) == 0

    expectation_validation_graph.update(validation_graph_with_single_edge)

    assert len(expectation_validation_graph.graph.edges) == 1


@pytest.mark.unit
def test_ExpectationValidationGraph_get_exception_info(
    expectation_validation_graph: ExpectationValidationGraph,
    validation_graph_with_single_edge: ValidationGraph,
    metric_edge: MetricEdge,
) -> None:
    left = metric_edge.left
    right = metric_edge.right

    left_exception = ExceptionInfo(
        exception_traceback="my first traceback",
        exception_message="my first message",
    )
    right_exception = ExceptionInfo(
        exception_traceback="my second traceback",
        exception_message="my second message",
        raised_exception=False,
    )

    metric_info = {
        left.id: {"exception_info": {left_exception}},
        right.id: {"exception_info": {right_exception}},
    }

    expectation_validation_graph.update(validation_graph_with_single_edge)
    exception_info = expectation_validation_graph.get_exception_info(
        metric_info=metric_info
    )

    assert left_exception in exception_info
    assert right_exception in exception_info


@pytest.mark.integration
def test_parse_validation_graph():
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={
            "column": "a",
            "mostly": 0.9,
            "threshold": 4,
            "double_sided": True,
        },
    )
    execution_engine = PandasExecutionEngine()
    graph = ValidationGraph(execution_engine=execution_engine)
    for configuration in [expectation_configuration]:
        expectation_impl = get_expectation_impl(
            "expect_column_value_z_scores_to_be_less_than"
        )
        validation_dependencies = expectation_impl(
            configuration
        ).get_validation_dependencies(configuration, execution_engine)

        for metric_configuration in validation_dependencies["metrics"].values():
            graph.build_metric_dependency_graph(
                metric_configuration=metric_configuration,
                runtime_configuration=None,
            )

    ready_metrics, needed_metrics = graph._parse(metrics=dict())
    assert len(ready_metrics) == 2 and len(needed_metrics) == 9


# Should be passing tests even if given incorrect MetricProvider data
@pytest.mark.integration
def test_parse_validation_graph_with_bad_metrics_args():
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={
            "column": "a",
            "mostly": 0.9,
            "threshold": 4,
            "double_sided": True,
        },
    )
    execution_engine = PandasExecutionEngine()
    graph = ValidationGraph(execution_engine=execution_engine)
    for configuration in [expectation_configuration]:
        expectation_impl = get_expectation_impl(
            "expect_column_value_z_scores_to_be_less_than"
        )
        validation_dependencies = expectation_impl(
            configuration
        ).get_validation_dependencies(
            configuration,
            execution_engine=execution_engine,
        )

        for metric_configuration in validation_dependencies["metrics"].values():
            graph.build_metric_dependency_graph(
                metric_configuration=metric_configuration,
                runtime_configuration=None,
            )

    # noinspection PyTypeChecker
    ready_metrics, needed_metrics = graph._parse(metrics=("nonexistent", "NONE"))
    assert len(ready_metrics) == 2 and len(needed_metrics) == 9


@pytest.mark.integration
def test_populate_dependencies():
    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_value_z_scores_to_be_less_than",
        kwargs={
            "column": "a",
            "mostly": 0.9,
            "threshold": 4,
            "double_sided": True,
        },
    )
    execution_engine = PandasExecutionEngine()
    graph = ValidationGraph(execution_engine=execution_engine)
    for configuration in [expectation_configuration]:
        expectation_impl = get_expectation_impl(
            "expect_column_value_z_scores_to_be_less_than"
        )
        validation_dependencies = expectation_impl(
            configuration
        ).get_validation_dependencies(
            configuration,
            execution_engine,
        )

        for metric_configuration in validation_dependencies["metrics"].values():
            graph.build_metric_dependency_graph(
                metric_configuration=metric_configuration,
            )

    assert len(graph.edges) == 33


@pytest.mark.integration
def test_populate_dependencies_with_incorrect_metric_name():
    execution_engine = PandasExecutionEngine()
    graph = ValidationGraph(execution_engine=execution_engine)
    try:
        graph.build_metric_dependency_graph(
            metric_configuration=MetricConfiguration(
                "column_values.not_a_metric", IDDict()
            ),
        )
    except ge_exceptions.MetricProviderError as e:
        graph = e

    assert isinstance(graph, ge_exceptions.MetricProviderError)


@pytest.mark.integration
def test_resolve_validation_graph_with_bad_config_catch_exceptions_true(
    basic_datasource,
):
    df = pd.DataFrame({"a": [1, 5, 22, 3, 5, 10], "b": [1, 2, 3, 4, 5, None]})

    batch = basic_datasource.get_single_batch_from_batch_request(
        RuntimeBatchRequest(
            **{
                "datasource_name": "my_datasource",
                "data_connector_name": "test_runtime_data_connector",
                "data_asset_name": "IN_MEMORY_DATA_ASSET",
                "runtime_parameters": {
                    "batch_data": df,
                },
                "batch_identifiers": {
                    "pipeline_stage_name": 0,
                    "airflow_run_id": 0,
                    "custom_key_0": 0,
                },
            }
        )
    )

    expectation_configuration = ExpectationConfiguration(
        expectation_type="expect_column_max_to_be_between",
        kwargs={"column": "not_in_table", "min_value": 1, "max_value": 29},
    )

    runtime_configuration = {
        "catch_exceptions": True,
        "result_format": {"result_format": "BASIC"},
    }

    execution_engine = PandasExecutionEngine()

    # noinspection PyUnusedLocal
    validator = Validator(execution_engine=execution_engine, batches=[batch])

    expectation_impl = get_expectation_impl(expectation_configuration.expectation_type)
    validation_dependencies = expectation_impl().get_validation_dependencies(
        expectation_configuration, execution_engine, runtime_configuration
    )["metrics"]

    graph = ValidationGraph(execution_engine=execution_engine)

    for metric_configuration in validation_dependencies.values():
        graph.build_metric_dependency_graph(
            metric_configuration=metric_configuration,
            runtime_configuration=runtime_configuration,
        )

    metrics: Dict[Tuple[str, str, str], Any] = {}
    aborted_metrics_info: Dict[
        Tuple[str, str, str],
        Dict[str, Union[MetricConfiguration, Set[ExceptionInfo], int]],
    ] = graph.resolve_validation_graph(
        metrics=metrics,
        runtime_configuration=runtime_configuration,
    )

    assert len(aborted_metrics_info) == 1

    aborted_metric_info_item = list(aborted_metrics_info.values())[0]
    assert aborted_metric_info_item["num_failures"] == MAX_METRIC_COMPUTATION_RETRIES

    assert len(aborted_metric_info_item["exception_info"]) == 1

    exception_info = next(iter(aborted_metric_info_item["exception_info"]))
    assert (
        exception_info["exception_message"]
        == 'Error: The column "not_in_table" in BatchData does not exist.'
    )


@pytest.mark.unit
@mock.patch("great_expectations.validator.validation_graph.tqdm")
def test_progress_bar_config_enabled(mock_tqdm: mock.MagicMock):
    class DummyMetricConfiguration:
        pass

    class DummyExecutionEngine:
        pass

    dummy_metric_configuration = cast(MetricConfiguration, DummyMetricConfiguration)
    dummy_execution_engine = cast(ExecutionEngine, DummyExecutionEngine)

    # ValidationGraph is a complex object that requires len > 3 to not trigger tqdm
    with mock.patch(
        "great_expectations.validator.validation_graph.ValidationGraph._parse",
        return_value=(
            {},
            {},
        ),
    ), mock.patch(
        "great_expectations.validator.validation_graph.ValidationGraph.edges",
        new_callable=mock.PropertyMock,
        return_value=[
            MetricEdge(left=dummy_metric_configuration),
            MetricEdge(left=dummy_metric_configuration),
            MetricEdge(left=dummy_metric_configuration),
        ],
    ):
        graph = ValidationGraph(execution_engine=dummy_execution_engine)
        graph.resolve_validation_graph(
            metrics={},
            runtime_configuration=None,
        )

    # Still invoked but doesn't actually do anything due to `disabled`
    assert mock_tqdm.called is True
    assert mock_tqdm.call_args[1]["disable"] is False


@pytest.mark.unit
@mock.patch("great_expectations.validator.validation_graph.tqdm")
def test_progress_bar_config_disabled(mock_tqdm: mock.MagicMock):
    class DummyMetricConfiguration:
        pass

    class DummyExecutionEngine:
        pass

    dummy_metric_configuration = cast(MetricConfiguration, DummyMetricConfiguration)
    dummy_execution_engine = cast(ExecutionEngine, DummyExecutionEngine)

    # ValidationGraph is a complex object that requires len > 3 to not trigger tqdm
    with mock.patch(
        "great_expectations.validator.validation_graph.ValidationGraph._parse",
        return_value=(
            {},
            {},
        ),
    ), mock.patch(
        "great_expectations.validator.validation_graph.ValidationGraph.edges",
        new_callable=mock.PropertyMock,
        return_value=[
            MetricEdge(left=dummy_metric_configuration),
            MetricEdge(left=dummy_metric_configuration),
            MetricEdge(left=dummy_metric_configuration),
        ],
    ):
        graph = ValidationGraph(execution_engine=dummy_execution_engine)
        graph.resolve_validation_graph(
            metrics={},
            runtime_configuration=None,
            show_progress_bars=False,
        )

    assert mock_tqdm.called is True
    assert mock_tqdm.call_args[1]["disable"] is True
