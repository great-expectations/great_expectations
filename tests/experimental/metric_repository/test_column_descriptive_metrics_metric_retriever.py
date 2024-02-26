from typing import List
from unittest import mock
from unittest.mock import Mock

import pytest

from great_expectations.data_context import CloudDataContext
from great_expectations.datasource.fluent import BatchRequest
from great_expectations.datasource.fluent.interfaces import Batch
from great_expectations.experimental.metric_repository.column_descriptive_metrics_metric_retriever import (
    ColumnDescriptiveMetricsMetricRetriever,
)
from great_expectations.experimental.metric_repository.metrics import (
    ColumnMetric,
    MetricException,
    TableMetric,
)
from great_expectations.rule_based_profiler.domain_builder import ColumnDomainBuilder
from great_expectations.validator.exception_info import ExceptionInfo
from great_expectations.validator.validator import Validator

pytestmark = pytest.mark.unit


def test_get_metrics():
    mock_context = Mock(spec=CloudDataContext)
    mock_validator = Mock(spec=Validator)
    mock_context.get_validator.return_value = mock_validator
    computed_metrics = {
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
    aborted_metrics = {}
    mock_validator.compute_metrics.return_value = (
        computed_metrics,
        aborted_metrics,
    )
    mock_batch = Mock(spec=Batch)
    mock_batch.id = "batch_id"
    mock_validator.active_batch = mock_batch

    metric_retriever = ColumnDescriptiveMetricsMetricRetriever(context=mock_context)

    mock_batch_request = Mock(spec=BatchRequest)

    with mock.patch(
        f"{ColumnDescriptiveMetricsMetricRetriever.__module__}.{ColumnDescriptiveMetricsMetricRetriever.__name__}._get_numeric_column_names",
        return_value=["col1", "col2"],
    ), mock.patch(
        f"{ColumnDescriptiveMetricsMetricRetriever.__module__}.{ColumnDescriptiveMetricsMetricRetriever.__name__}._get_timestamp_column_names",
        return_value=[],
    ):
        metrics = metric_retriever.get_metrics(batch_request=mock_batch_request)

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


def test_get_metrics_metrics_missing():
    """This test is meant to simulate metrics missing from the computed metrics."""
    mock_context = Mock(spec=CloudDataContext)
    mock_validator = Mock(spec=Validator)
    mock_context.get_validator.return_value = mock_validator
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
    mock_validator.compute_metrics.return_value = (
        mock_computed_metrics,
        mock_aborted_metrics,
    )
    mock_batch = Mock(spec=Batch)
    mock_batch.id = "batch_id"
    mock_validator.active_batch = mock_batch

    metric_retriever = ColumnDescriptiveMetricsMetricRetriever(context=mock_context)

    mock_batch_request = Mock(spec=BatchRequest)

    with mock.patch(
        f"{ColumnDescriptiveMetricsMetricRetriever.__module__}.{ColumnDescriptiveMetricsMetricRetriever.__name__}._get_numeric_column_names",
        return_value=["col1", "col2"],
    ), mock.patch(
        f"{ColumnDescriptiveMetricsMetricRetriever.__module__}.{ColumnDescriptiveMetricsMetricRetriever.__name__}._get_timestamp_column_names",
        return_value=[],
    ):
        metrics = metric_retriever.get_metrics(batch_request=mock_batch_request)

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


def test_get_metrics_with_exception():
    """This test is meant to simulate failed metrics in the computed metrics."""
    mock_context = Mock(spec=CloudDataContext)
    mock_validator = Mock(spec=Validator)
    mock_context.get_validator.return_value = mock_validator

    exception_info = ExceptionInfo(
        exception_traceback="test exception traceback",
        exception_message="test exception message",
        raised_exception=True,
    )

    aborted_metrics = {
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

    computed_metrics = {
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
    mock_validator.compute_metrics.return_value = (
        computed_metrics,
        aborted_metrics,
    )
    mock_batch = Mock(spec=Batch)
    mock_batch.id = "batch_id"
    mock_validator.active_batch = mock_batch

    metric_retriever = ColumnDescriptiveMetricsMetricRetriever(context=mock_context)

    mock_batch_request = Mock(spec=BatchRequest)

    with mock.patch(
        f"{ColumnDescriptiveMetricsMetricRetriever.__module__}.{ColumnDescriptiveMetricsMetricRetriever.__name__}._get_numeric_column_names",
        return_value=["col1", "col2"],
    ), mock.patch(
        f"{ColumnDescriptiveMetricsMetricRetriever.__module__}.{ColumnDescriptiveMetricsMetricRetriever.__name__}._get_timestamp_column_names",
        return_value=[],
    ):
        metrics = metric_retriever.get_metrics(batch_request=mock_batch_request)

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


def test_get_metrics_with_column_type_missing():
    """This test is meant to simulate failed metrics in the computed metrics."""
    mock_context = Mock(spec=CloudDataContext)
    mock_validator = Mock(spec=Validator)
    mock_context.get_validator.return_value = mock_validator

    exception_info = ExceptionInfo(
        exception_traceback="test exception traceback",
        exception_message="test exception message",
        raised_exception=True,
    )

    aborted_metrics = {
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

    computed_metrics = {
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
    mock_validator.compute_metrics.return_value = (
        computed_metrics,
        aborted_metrics,
    )
    mock_batch = Mock(spec=Batch)
    mock_batch.id = "batch_id"
    mock_validator.active_batch = mock_batch

    metric_retriever = ColumnDescriptiveMetricsMetricRetriever(context=mock_context)

    mock_batch_request = Mock(spec=BatchRequest)

    with mock.patch(
        f"{ColumnDescriptiveMetricsMetricRetriever.__module__}.{ColumnDescriptiveMetricsMetricRetriever.__name__}._get_numeric_column_names",
        return_value=["col1", "col2"],
    ), mock.patch(
        f"{ColumnDescriptiveMetricsMetricRetriever.__module__}.{ColumnDescriptiveMetricsMetricRetriever.__name__}._get_timestamp_column_names",
        return_value=[],
    ):
        metrics = metric_retriever.get_metrics(batch_request=mock_batch_request)

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


def test_get_metrics_with_timestamp_columns():
    mock_context = Mock(spec=CloudDataContext)
    mock_validator = Mock(spec=Validator)
    mock_context.get_validator.return_value = mock_validator
    computed_metrics = {
        ("table.row_count", (), ()): 2,
        ("table.columns", (), ()): ["timestamp_col"],
        ("table.column_types", (), "include_nested=True"): [
            {"name": "timestamp_col", "type": "TIMESTAMP_NTZ"},
        ],
        ("column.min", "column=timestamp_col", ()): "2023-01-01T00:00:00",
        ("column.max", "column=timestamp_col", ()): "2023-12-31T00:00:00",
        ("column_values.null.count", "column=timestamp_col", ()): 1,
    }
    aborted_metrics = {}
    mock_validator.compute_metrics.return_value = (
        computed_metrics,
        aborted_metrics,
    )
    mock_batch = Mock(spec=Batch)
    mock_batch.id = "batch_id"
    mock_validator.active_batch = mock_batch

    metric_retriever = ColumnDescriptiveMetricsMetricRetriever(context=mock_context)

    mock_batch_request = Mock(spec=BatchRequest)

    with mock.patch(
        f"{ColumnDescriptiveMetricsMetricRetriever.__module__}.{ColumnDescriptiveMetricsMetricRetriever.__name__}._get_numeric_column_names",
        return_value=[],
    ), mock.patch(
        f"{ColumnDescriptiveMetricsMetricRetriever.__module__}.{ColumnDescriptiveMetricsMetricRetriever.__name__}._get_timestamp_column_names",
        return_value=["timestamp_col"],
    ):
        metrics = metric_retriever.get_metrics(batch_request=mock_batch_request)

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


def test_get_metrics_only_gets_a_validator_once():
    mock_context = Mock(spec=CloudDataContext)
    mock_validator = Mock(spec=Validator)
    mock_context.get_validator.return_value = mock_validator

    aborted_metrics = {}

    computed_metrics = {
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
    mock_validator.compute_metrics.return_value = (
        computed_metrics,
        aborted_metrics,
    )
    mock_batch = Mock(spec=Batch)
    mock_batch.id = "batch_id"
    mock_validator.active_batch = mock_batch

    metric_retriever = ColumnDescriptiveMetricsMetricRetriever(context=mock_context)

    mock_batch_request = Mock(spec=BatchRequest)

    with mock.patch(
        f"{ColumnDomainBuilder.__module__}.{ColumnDomainBuilder.__name__}.get_effective_column_names",
        return_value=["col1", "col2"],
    ):
        metric_retriever.get_metrics(batch_request=mock_batch_request)

    mock_context.get_validator.assert_called_once_with(batch_request=mock_batch_request)
