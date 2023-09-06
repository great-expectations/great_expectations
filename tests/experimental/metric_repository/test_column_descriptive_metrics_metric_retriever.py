from typing import List
from unittest.mock import Mock

import pytest

from great_expectations.data_context import CloudDataContext
from great_expectations.datasource.fluent import BatchRequest
from great_expectations.datasource.fluent.interfaces import Batch
from great_expectations.experimental.metric_repository.column_descriptive_metrics_metric_retriever import (
    ColumnDescriptiveMetricsMetricRetriever,
)
from great_expectations.experimental.metric_repository.metrics import TableMetric
from great_expectations.validator.validator import Validator

pytestmark = pytest.mark.unit


def test_get_metrics():
    mock_context = Mock(spec=CloudDataContext)
    mock_validator = Mock(spec=Validator)
    mock_context.get_validator.return_value = mock_validator
    mock_validator.compute_metrics.return_value = {
        ("table.row_count", (), ()): 2,
        ("table.columns", (), ()): ["col1", "col2"],
    }
    mock_batch = Mock(spec=Batch)
    mock_batch.id = "batch_id"
    mock_validator.active_batch = mock_batch

    metric_retriever = ColumnDescriptiveMetricsMetricRetriever(context=mock_context)

    mock_batch_request = Mock(spec=BatchRequest)

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
    ]


def test_get_metrics_with_exception():
    # TODO: Implement this test in DX-749
    pass
