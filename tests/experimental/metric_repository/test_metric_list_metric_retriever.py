from typing import List
from unittest import mock
from unittest.mock import Mock

import pytest

from great_expectations.data_context import CloudDataContext
from great_expectations.datasource.fluent import BatchRequest
from great_expectations.datasource.fluent.interfaces import Batch
from great_expectations.experimental.metric_repository.metric_list_retriever import (
    MetricListMetricRetriever,
)
from great_expectations.experimental.metric_repository.metrics import (
    ColumnMetric,
    MetricTypes,
    TableMetric,
)
from great_expectations.validator.validator import Validator

pytestmark = pytest.mark.unit


def test_get_metrics_table_metrics_only():
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
    }
    table_metrics_list = [
        MetricTypes.TABLE_ROW_COUNT,
        MetricTypes.TABLE_COLUMNS,
        MetricTypes.TABLE_COLUMN_TYPES,
    ]
    aborted_metrics = {}
    mock_validator.compute_metrics.return_value = (
        computed_metrics,
        aborted_metrics,
    )
    mock_batch = Mock(spec=Batch)
    mock_batch.id = "batch_id"
    mock_validator.active_batch = mock_batch

    metric_retriever = MetricListMetricRetriever(context=mock_context)

    mock_batch_request = Mock(spec=BatchRequest)

    metrics = metric_retriever.get_metrics(
        batch_request=mock_batch_request,
        metric_list=table_metrics_list,
    )
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
    ]


def test_get_metrics_full_list():
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
    cdm_metrics_list = [
        MetricTypes.TABLE_ROW_COUNT,
        MetricTypes.TABLE_COLUMNS,
        MetricTypes.TABLE_COLUMN_TYPES,
        MetricTypes.COLUMN_MIN,
        MetricTypes.COLUMN_MAX,
        MetricTypes.COLUMN_MEAN,
        MetricTypes.COLUMN_MEDIAN,
        MetricTypes.COLUMN_NULL_COUNT,
    ]
    aborted_metrics = {}
    mock_validator.compute_metrics.return_value = (
        computed_metrics,
        aborted_metrics,
    )
    mock_batch = Mock(spec=Batch)
    mock_batch.id = "batch_id"
    mock_validator.active_batch = mock_batch

    metric_retriever = MetricListMetricRetriever(context=mock_context)

    mock_batch_request = Mock(spec=BatchRequest)

    with mock.patch(
        f"{MetricListMetricRetriever.__module__}.{MetricListMetricRetriever.__name__}._get_numeric_column_names",
        return_value=["col1", "col2"],
    ), mock.patch(
        f"{MetricListMetricRetriever.__module__}.{MetricListMetricRetriever.__name__}._get_timestamp_column_names",
        return_value=[],
    ):
        metrics = metric_retriever.get_metrics(
            batch_request=mock_batch_request,
            metric_list=cdm_metrics_list,
        )

    assert metrics == [
        TableMetric[int](
            batch_id="batch_id", metric_name="table.row_count", value=2, exception=None
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
            metric_name="column.max",
            value=5.5,
            exception=None,
            column="col1",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.max",
            value=5.7,
            exception=None,
            column="col2",
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
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.min",
            value=2.5,
            exception=None,
            column="col1",
        ),
        ColumnMetric[float](
            batch_id="batch_id",
            metric_name="column.min",
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
    pass


def test_get_metrics_with_exception():
    """This test is meant to simulate failed metrics in the computed metrics."""
    pass


def test_get_metrics_with_column_type_missing():
    """This test is meant to simulate failed metrics in the computed metrics."""
    pass


def test_get_metrics_with_timestamp_columns():
    pass


def test_get_metrics_only_gets_a_validator_once():
    pass
