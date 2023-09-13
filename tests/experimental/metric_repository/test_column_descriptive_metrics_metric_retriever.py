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
from great_expectations.validator.validator import Validator

pytestmark = pytest.mark.unit


def test_get_metrics():
    mock_context = Mock(spec=CloudDataContext)
    mock_validator = Mock(spec=Validator)
    mock_context.get_validator.return_value = mock_validator
    mock_validator.compute_metrics.return_value = {
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
    mock_batch = Mock(spec=Batch)
    mock_batch.id = "batch_id"
    mock_validator.active_batch = mock_batch

    metric_retriever = ColumnDescriptiveMetricsMetricRetriever(context=mock_context)

    mock_batch_request = Mock(spec=BatchRequest)

    with mock.patch(
        f"{ColumnDomainBuilder.__module__}.{ColumnDomainBuilder.__name__}.get_effective_column_names",
        return_value=["col1", "col2"],
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
    """This test is meant to simulate a single metric missing from the computed metrics."""
    mock_context = Mock(spec=CloudDataContext)
    mock_validator = Mock(spec=Validator)
    mock_context.get_validator.return_value = mock_validator
    mock_validator.compute_metrics.return_value = {
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
    mock_batch = Mock(spec=Batch)
    mock_batch.id = "batch_id"
    mock_validator.active_batch = mock_batch

    metric_retriever = ColumnDescriptiveMetricsMetricRetriever(context=mock_context)

    mock_batch_request = Mock(spec=BatchRequest)

    with mock.patch(
        f"{ColumnDomainBuilder.__module__}.{ColumnDomainBuilder.__name__}.get_effective_column_names",
        return_value=["col1", "col2"],
    ):
        metrics = metric_retriever.get_metrics(batch_request=mock_batch_request)

    assert metrics == [
        TableMetric[int](
            batch_id="batch_id",
            metric_name="table.row_count",
            value=None,
            exception=MetricException(type="TBD", message="TBD"),
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
            exception=MetricException(type="TBD", message="TBD"),
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
