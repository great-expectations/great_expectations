from typing import Optional

import pytest

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.validator.exception_info import ExceptionInfo
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.validation_graph import (
    ExpectationValidationGraph,
    MetricEdge,
    ValidationGraph,
)


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
