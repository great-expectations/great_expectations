from typing import Callable, List
from unittest.mock import Mock  # noqa: TID251

import pytest

from great_expectations.data_context import CloudDataContext
from great_expectations.datasource.fluent import BatchRequest
from great_expectations.datasource.fluent.interfaces import Batch
from great_expectations.experimental.metric_repository.column_descriptive_metrics_metric_retriever import (  # noqa: E501
    ColumnDescriptiveMetricsMetricRetriever,
)
from great_expectations.experimental.metric_repository.metrics import (
    ColumnMetric,
    MetricException,
    TableMetric,
)
from great_expectations.validator.exception_info import ExceptionInfo
from great_expectations.validator.metrics_calculator import (
    _AbortedMetricsInfoDict,
    _MetricsDict,
)
from great_expectations.validator.validator import Validator

pytestmark = pytest.mark.unit


@pytest.fixture
def mock_batch():
    mock_batch = Mock(spec=Batch)
    mock_batch.id = "batch_id"
    return mock_batch


@pytest.fixture
def construct_mock_validator(mock_batch: Batch):
    def _construct_mock_validator(
        computed_metrics: _MetricsDict,
        aborted_metrics: _AbortedMetricsInfoDict,
    ):
        mock_validator = Mock(spec=Validator)
        mock_validator.compute_metrics.return_value = (
            computed_metrics,
            aborted_metrics,
        )
        mock_validator.active_batch = mock_batch
        return mock_validator

    return _construct_mock_validator


@pytest.fixture
def construct_mock_context(construct_mock_validator: Callable):
    def _construct_mock_context(
        computed_metrics: _MetricsDict,
        aborted_metrics: _AbortedMetricsInfoDict,
    ):
        mock_context = Mock(spec=CloudDataContext)
        mock_validator = construct_mock_validator(
            computed_metrics=computed_metrics,
            aborted_metrics=aborted_metrics,
        )
        mock_context.get_validator.return_value = mock_validator
        return mock_context

    return _construct_mock_context


@pytest.fixture
def construct_patched_metric_retriever(construct_mock_context: Callable):
    """Patch methods that are not relevant to the tests."""

    def _construct_mock_metric_retriever(
        computed_metrics: _MetricsDict,
        aborted_metrics: _AbortedMetricsInfoDict,
        numeric_column_names: List[str],
        timestamp_column_names: List[str],
        return_mock_context: bool = False,
    ):
        mock_context = construct_mock_context(
            computed_metrics=computed_metrics,
            aborted_metrics=aborted_metrics,
        )

        def mock__get_numeric_column_names(
            batch_request: BatchRequest,
            exclude_column_names: List[str],
        ):
            return numeric_column_names

        def mock__get_timestamp_column_names(
            batch_request: BatchRequest,
            exclude_column_names: List[str],
        ):
            return timestamp_column_names

        metric_retriever = ColumnDescriptiveMetricsMetricRetriever(context=mock_context)
        metric_retriever._get_numeric_column_names = mock__get_numeric_column_names
        metric_retriever._get_timestamp_column_names = mock__get_timestamp_column_names
        if return_mock_context:
            return metric_retriever, mock_context
        else:
            return metric_retriever

    return _construct_mock_metric_retriever


def test_get_metrics(
    construct_patched_metric_retriever: Callable,
):
    mock_computed_metrics = {
        ("table.row_count", (), ()): 2,
        ("table.columns", (), ()): ["col1", "col2"],
        ("table.column_types", (), "include_nested=True"): [
            {"name": "col1", "type": "float"},
            {"name": "col2", "type": "float"},
        ],
        ("column.min", "column=col1", ()): 2.5,
        ("column.min", "column=col2", ()): 2.7,
        ("column.max", "column=col1", ()): 5.5,
        ("column.max", "column=col2", ()): 5.7,
        ("column.mean", "column=col1", ()): 2.5,
        ("column.mean", "column=col2", ()): 2.7,
        ("column.median", "column=col1", ()): 2.5,
        ("column.median", "column=col2", ()): 2.7,
        ("column_values.null.count", "column=col1", ()): 1,
        ("column_values.null.count", "column=col2", ()): 1,
    }
    mock_aborted_metrics = {}

    mock_metric_retriever = construct_patched_metric_retriever(
        computed_metrics=mock_computed_metrics,
        aborted_metrics=mock_aborted_metrics,
        numeric_column_names=["col1", "col2"],
        timestamp_column_names=[],  # No timestamp columns
    )
    metrics = mock_metric_retriever.get_metrics(batch_request=Mock(spec=BatchRequest))

    assert metrics == [
        TableMetric[int](
            batch_id="batch_id",
            metric_name="table.row_count",
            value=2,
            exception=None,
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.columns",
            value=["col1", "col2"],
            exception=None,
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.column_types",
            value=[
                {"name": "col1", "type": "float"},
                {"name": "col2", "type": "float"},
            ],
            exception=None,
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.min",
            column="col1",
            value=2.5,
            exception=None,
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.min",
            column="col2",
            value=2.7,
            exception=None,
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.max",
            column="col1",
            value=5.5,
            exception=None,
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.max",
            column="col2",
            value=5.7,
            exception=None,
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.mean",
            value=2.5,
            exception=None,
            column="col1",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.mean",
            value=2.7,
            exception=None,
            column="col2",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.median",
            value=2.5,
            exception=None,
            column="col1",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.median",
            value=2.7,
            exception=None,
            column="col2",
        ),
        ColumnMetric[int](
            batch_id="batch_id",
            metric_name="column_values.null.count",
            value=1,
            exception=None,
            column="col1",
        ),
        ColumnMetric[int](
            batch_id="batch_id",
            metric_name="column_values.null.count",
            value=1,
            exception=None,
            column="col2",
        ),
    ]


def test_get_metrics_metrics_missing(
    construct_patched_metric_retriever: Callable,
):
    """This test is meant to simulate metrics missing from the computed metrics."""
    mock_computed_metrics = {
        # ("table.row_count", (), ()): 2, # Missing table.row_count metric
        ("table.columns", (), ()): ["col1", "col2"],
        ("table.column_types", (), "include_nested=True"): [
            {"name": "col1", "type": "float"},
            {"name": "col2", "type": "float"},
        ],
        # ("column.min", "column=col1", ()): 2.5, # Missing column.min metric for col1
        ("column.min", "column=col2", ()): 2.7,
        ("column.max", "column=col1", ()): 5.5,
        ("column.max", "column=col2", ()): 5.7,
        ("column.mean", "column=col1", ()): 2.5,
        ("column.mean", "column=col2", ()): 2.7,
        ("column.median", "column=col1", ()): 2.5,
        ("column.median", "column=col2", ()): 2.7,
        ("column_values.null.count", "column=col1", ()): 1,
        ("column_values.null.count", "column=col2", ()): 1,
    }
    mock_aborted_metrics = {}
    mock_metric_retriever = construct_patched_metric_retriever(
        computed_metrics=mock_computed_metrics,
        aborted_metrics=mock_aborted_metrics,
        numeric_column_names=["col1", "col2"],
        timestamp_column_names=[],  # No timestamp columns
    )
    metrics = mock_metric_retriever.get_metrics(batch_request=Mock(spec=BatchRequest))

    assert metrics == [
        TableMetric[int](
            batch_id="batch_id",
            metric_name="table.row_count",
            value=None,
            exception=MetricException(
                type="Not found",
                message="Metric was not successfully computed but exception was not found.",
            ),
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.columns",
            value=["col1", "col2"],
            exception=None,
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.column_types",
            value=[
                {"name": "col1", "type": "float"},
                {"name": "col2", "type": "float"},
            ],
            exception=None,
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.min",
            column="col1",
            value=None,
            exception=MetricException(
                type="Not found",
                message="Metric was not successfully computed but exception was not found.",
            ),
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.min",
            column="col2",
            value=2.7,
            exception=None,
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.max",
            column="col1",
            value=5.5,
            exception=None,
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.max",
            column="col2",
            value=5.7,
            exception=None,
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.mean",
            value=2.5,
            exception=None,
            column="col1",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.mean",
            value=2.7,
            exception=None,
            column="col2",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.median",
            value=2.5,
            exception=None,
            column="col1",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.median",
            value=2.7,
            exception=None,
            column="col2",
        ),
        ColumnMetric[int](
            batch_id="batch_id",
            metric_name="column_values.null.count",
            value=1,
            exception=None,
            column="col1",
        ),
        ColumnMetric[int](
            batch_id="batch_id",
            metric_name="column_values.null.count",
            value=1,
            exception=None,
            column="col2",
        ),
    ]


def test_get_metrics_with_exception(
    construct_patched_metric_retriever: Callable,
):
    """This test is meant to simulate failed metrics in the computed metrics."""

    exception_info = ExceptionInfo(
        exception_traceback="test exception traceback",
        exception_message="test exception message",
        raised_exception=True,
    )

    mock_aborted_metrics = {
        ("table.row_count", (), ()): {
            "metric_configuration": {},  # Leaving out for brevity
            "num_failures": 3,
            "exception_info": exception_info,
        },
        ("column.min", "column=col1", ()): {
            "metric_configuration": {},  # Leaving out for brevity
            "num_failures": 3,
            "exception_info": exception_info,
        },
    }

    mock_computed_metrics = {
        # ("table.row_count", (), ()): 2, # Error in table.row_count metric
        ("table.columns", (), ()): ["col1", "col2"],
        ("table.column_types", (), "include_nested=True"): [
            {"name": "col1", "type": "float"},
            {"name": "col2", "type": "float"},
        ],
        # ("column.min", "column=col1", ()): 2.5, # Error in column.min metric for col1
        ("column.min", "column=col2", ()): 2.7,
        ("column.max", "column=col1", ()): 5.5,
        ("column.max", "column=col2", ()): 5.7,
        ("column.mean", "column=col1", ()): 2.5,
        ("column.mean", "column=col2", ()): 2.7,
        ("column.median", "column=col1", ()): 2.5,
        ("column.median", "column=col2", ()): 2.7,
        ("column_values.null.count", "column=col1", ()): 1,
        ("column_values.null.count", "column=col2", ()): 1,
    }
    mock_metric_retriever = construct_patched_metric_retriever(
        computed_metrics=mock_computed_metrics,
        aborted_metrics=mock_aborted_metrics,
        numeric_column_names=["col1", "col2"],
        timestamp_column_names=[],  # No timestamp columns
    )
    metrics = mock_metric_retriever.get_metrics(batch_request=Mock(spec=BatchRequest))

    assert metrics == [
        TableMetric[int](
            batch_id="batch_id",
            metric_name="table.row_count",
            value=None,
            exception=MetricException(type="Unknown", message="test exception message"),
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.columns",
            value=["col1", "col2"],
            exception=None,
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.column_types",
            value=[
                {"name": "col1", "type": "float"},
                {"name": "col2", "type": "float"},
            ],
            exception=None,
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.min",
            column="col1",
            value=None,
            exception=MetricException(type="Unknown", message="test exception message"),
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.min",
            column="col2",
            value=2.7,
            exception=None,
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.max",
            column="col1",
            value=5.5,
            exception=None,
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.max",
            column="col2",
            value=5.7,
            exception=None,
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.mean",
            value=2.5,
            exception=None,
            column="col1",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.mean",
            value=2.7,
            exception=None,
            column="col2",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.median",
            value=2.5,
            exception=None,
            column="col1",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.median",
            value=2.7,
            exception=None,
            column="col2",
        ),
        ColumnMetric[int](
            batch_id="batch_id",
            metric_name="column_values.null.count",
            value=1,
            exception=None,
            column="col1",
        ),
        ColumnMetric[int](
            batch_id="batch_id",
            metric_name="column_values.null.count",
            value=1,
            exception=None,
            column="col2",
        ),
    ]


def test_get_metrics_with_column_type_missing(
    construct_patched_metric_retriever: Callable,
):
    """This test is meant to simulate failed metrics in the computed metrics."""
    mock_context = Mock(spec=CloudDataContext)
    mock_validator = Mock(spec=Validator)
    mock_context.get_validator.return_value = mock_validator

    exception_info = ExceptionInfo(
        exception_traceback="test exception traceback",
        exception_message="test exception message",
        raised_exception=True,
    )

    mock_aborted_metrics = {
        ("table.row_count", (), ()): {
            "metric_configuration": {},  # Leaving out for brevity
            "num_failures": 3,
            "exception_info": exception_info,
        },
        ("column.min", "column=col1", ()): {
            "metric_configuration": {},  # Leaving out for brevity
            "num_failures": 3,
            "exception_info": exception_info,
        },
    }

    mock_computed_metrics = {
        # ("table.row_count", (), ()): 2, # Error in table.row_count metric
        ("table.columns", (), ()): ["col1", "col2"],
        ("table.column_types", (), "include_nested=True"): [
            {"name": "col1", "type": "float"},
            {
                "name": "col2",
            },  # Missing type for col2
        ],
        # ("column.min", "column=col1", ()): 2.5, # Error in column.min metric for col1
        ("column.min", "column=col2", ()): 2.7,
        ("column.max", "column=col1", ()): 5.5,
        ("column.max", "column=col2", ()): 5.7,
        ("column.mean", "column=col1", ()): 2.5,
        ("column.mean", "column=col2", ()): 2.7,
        ("column.median", "column=col1", ()): 2.5,
        ("column.median", "column=col2", ()): 2.7,
        ("column_values.null.count", "column=col1", ()): 1,
        ("column_values.null.count", "column=col2", ()): 1,
    }
    mock_metric_retriever = construct_patched_metric_retriever(
        computed_metrics=mock_computed_metrics,
        aborted_metrics=mock_aborted_metrics,
        numeric_column_names=["col1", "col2"],
        timestamp_column_names=[],  # No timestamp columns
    )
    metrics = mock_metric_retriever.get_metrics(batch_request=Mock(spec=BatchRequest))

    assert metrics == [
        TableMetric[int](
            batch_id="batch_id",
            metric_name="table.row_count",
            value=None,
            exception=MetricException(type="Unknown", message="test exception message"),
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.columns",
            value=["col1", "col2"],
            exception=None,
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.column_types",
            value=[
                {"name": "col1", "type": "float"},
                {
                    "name": "col2",
                },  # Note: No type for col2
            ],
            exception=None,
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.min",
            column="col1",
            value=None,
            exception=MetricException(type="Unknown", message="test exception message"),
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.min",
            column="col2",
            value=2.7,
            exception=None,
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.max",
            column="col1",
            value=5.5,
            exception=None,
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.max",
            column="col2",
            value=5.7,
            exception=None,
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.mean",
            value=2.5,
            exception=None,
            column="col1",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.mean",
            value=2.7,
            exception=None,
            column="col2",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.median",
            value=2.5,
            exception=None,
            column="col1",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.median",
            value=2.7,
            exception=None,
            column="col2",
        ),
        ColumnMetric[int](
            batch_id="batch_id",
            metric_name="column_values.null.count",
            value=1,
            exception=None,
            column="col1",
        ),
        ColumnMetric[int](
            batch_id="batch_id",
            metric_name="column_values.null.count",
            value=1,
            exception=None,
            column="col2",
        ),
    ]


def test_get_metrics_with_timestamp_columns(
    construct_patched_metric_retriever: Callable,
):
    mock_computed_metrics = {
        ("table.row_count", (), ()): 2,
        ("table.columns", (), ()): ["timestamp_col"],
        ("table.column_types", (), "include_nested=True"): [
            {"name": "timestamp_col", "type": "TIMESTAMP_NTZ"},
        ],
        ("column.min", "column=timestamp_col", ()): "2023-01-01T00:00:00",
        ("column.max", "column=timestamp_col", ()): "2023-12-31T00:00:00",
        ("column_values.null.count", "column=timestamp_col", ()): 1,
    }
    mock_aborted_metrics = {}
    mock_metric_retriever = construct_patched_metric_retriever(
        computed_metrics=mock_computed_metrics,
        aborted_metrics=mock_aborted_metrics,
        numeric_column_names=[],  # No numeric columns
        timestamp_column_names=["timestamp_col"],
    )
    metrics = mock_metric_retriever.get_metrics(batch_request=Mock(spec=BatchRequest))

    assert metrics == [
        TableMetric[int](
            batch_id="batch_id",
            metric_name="table.row_count",
            value=2,
            exception=None,
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.columns",
            value=["timestamp_col"],
            exception=None,
        ),
        TableMetric[List[str]](
            batch_id="batch_id",
            metric_name="table.column_types",
            value=[{"name": "timestamp_col", "type": "TIMESTAMP_NTZ"}],
            exception=None,
        ),
        ColumnMetric[str](
            batch_id="batch_id",
            metric_name="column.min",
            value="2023-01-01T00:00:00",
            exception=None,
            column="timestamp_col",
        ),
        ColumnMetric[str](
            batch_id="batch_id",
            metric_name="column.max",
            value="2023-12-31T00:00:00",
            exception=None,
            column="timestamp_col",
        ),
        ColumnMetric[int](
            batch_id="batch_id",
            metric_name="column_values.null.count",
            value=1,
            exception=None,
            column="timestamp_col",
        ),
    ]


def test_get_metrics_only_gets_a_validator_once(
    construct_patched_metric_retriever: Callable,
):
    mock_aborted_metrics = {}

    mock_computed_metrics = {
        ("table.row_count", (), ()): 2,
        ("table.columns", (), ()): ["col1", "col2"],
        ("table.column_types", (), "include_nested=True"): [
            {"name": "col1", "type": "float"},
            {"name": "col2", "type": "float"},
        ],
        ("column.min", "column=col1", ()): 2.5,
        ("column.min", "column=col2", ()): 2.7,
        ("column.max", "column=col1", ()): 5.5,
        ("column.max", "column=col2", ()): 5.7,
        ("column.mean", "column=col1", ()): 2.5,
        ("column.mean", "column=col2", ()): 2.7,
        ("column.median", "column=col1", ()): 2.5,
        ("column.median", "column=col2", ()): 2.7,
        ("column_values.null.count", "column=col1", ()): 1,
        ("column_values.null.count", "column=col2", ()): 1,
    }
    mock_metric_retriever, mock_context = construct_patched_metric_retriever(
        computed_metrics=mock_computed_metrics,
        aborted_metrics=mock_aborted_metrics,
        numeric_column_names=["col1", "col2"],
        timestamp_column_names=[],  # No timestamp columns
        return_mock_context=True,
    )
    mock_batch_request = Mock(spec=BatchRequest)
    mock_metric_retriever.get_metrics(batch_request=mock_batch_request)

    mock_context.get_validator.assert_called_once_with(batch_request=mock_batch_request)
