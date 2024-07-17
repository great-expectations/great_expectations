import uuid
from unittest.mock import MagicMock, Mock

import pytest

from great_expectations.data_context import CloudDataContext
from great_expectations.datasource.fluent import BatchRequest
from great_expectations.experimental.metric_repository.batch_inspector import (
    BatchInspector,
)
from great_expectations.experimental.metric_repository.metric_retriever import (
    MetricRetriever,
)
from great_expectations.experimental.metric_repository.metrics import (
    MetricRun,
    MetricTypes,
    TableMetric,
)

pytestmark = pytest.mark.unit


# compute_metric_list_run tests
def test_compute_metric_list_run_with_no_metric_retrievers(mocker):
    mock_context = Mock(spec=CloudDataContext)
    batch_inspector = BatchInspector(context=mock_context, metric_retrievers=[])
    mock_batch_request = Mock(spec=BatchRequest)

    data_asset_id = uuid.uuid4()

    metric_run = batch_inspector.compute_metric_list_run(
        data_asset_id=data_asset_id, batch_request=mock_batch_request, metric_list=[]
    )
    assert metric_run == MetricRun(data_asset_id=data_asset_id, metrics=[])


def test_compute_metric_list_run_calls_metric_retrievers():
    mock_context = Mock(spec=CloudDataContext)
    mock_metric_retriever = MagicMock(spec=MetricRetriever)
    batch_inspector = BatchInspector(
        context=mock_context, metric_retrievers=[mock_metric_retriever]
    )
    mock_batch_request = Mock(spec=BatchRequest)

    data_asset_id = uuid.uuid4()

    metric_list = [
        MetricTypes.TABLE_ROW_COUNT,
        MetricTypes.TABLE_COLUMNS,
        MetricTypes.TABLE_COLUMN_TYPES,
        MetricTypes.COLUMN_MIN,
        MetricTypes.COLUMN_MAX,
        MetricTypes.COLUMN_MEAN,
        MetricTypes.COLUMN_MEDIAN,
        MetricTypes.COLUMN_NULL_COUNT,
    ]

    batch_inspector.compute_metric_list_run(
        data_asset_id=data_asset_id,
        batch_request=mock_batch_request,
        metric_list=metric_list,
    )

    assert mock_metric_retriever.get_metrics.call_count == 1

    mock_metric_retriever.get_metrics.assert_called_once_with(
        batch_request=mock_batch_request, metric_list=metric_list
    )


def test_compute_metric_list_run_returns_metric_run():
    mock_context = Mock(spec=CloudDataContext)
    mock_metric_retriever = MagicMock(spec=MetricRetriever)

    mock_metric = Mock(spec=TableMetric)
    mock_metric_retriever.get_metrics.return_value = [mock_metric]

    batch_inspector = BatchInspector(
        context=mock_context, metric_retrievers=[mock_metric_retriever]
    )
    mock_batch_request = Mock(spec=BatchRequest)

    data_asset_id = uuid.uuid4()

    metric_run = batch_inspector.compute_metric_list_run(
        data_asset_id=data_asset_id,
        batch_request=mock_batch_request,
        metric_list=[
            MetricTypes.TABLE_ROW_COUNT,
            MetricTypes.TABLE_COLUMNS,
            MetricTypes.TABLE_COLUMN_TYPES,
            MetricTypes.COLUMN_MIN,
            MetricTypes.COLUMN_MAX,
            MetricTypes.COLUMN_MEAN,
            MetricTypes.COLUMN_MEDIAN,
            MetricTypes.COLUMN_NULL_COUNT,
        ],
    )

    assert metric_run == MetricRun(
        data_asset_id=data_asset_id,
        metrics=[mock_metric],
    )


# compute_metric_run tests. Will eventually go away once compute_metric_list_run is fully implemented.
def test_compute_metric_run_with_no_metric_retrievers():
    mock_context = Mock(spec=CloudDataContext)
    batch_inspector = BatchInspector(context=mock_context, metric_retrievers=[])
    mock_batch_request = Mock(spec=BatchRequest)

    data_asset_id = uuid.uuid4()

    metric_run = batch_inspector.compute_metric_run(
        data_asset_id=data_asset_id, batch_request=mock_batch_request
    )
    assert metric_run == MetricRun(data_asset_id=data_asset_id, metrics=[])


def test_compute_metric_run_calls_metric_retrievers():
    mock_context = Mock(spec=CloudDataContext)
    mock_metric_retriever = MagicMock(spec=MetricRetriever)
    batch_inspector = BatchInspector(
        context=mock_context, metric_retrievers=[mock_metric_retriever]
    )
    mock_batch_request = Mock(spec=BatchRequest)

    data_asset_id = uuid.uuid4()

    batch_inspector.compute_metric_run(
        data_asset_id=data_asset_id, batch_request=mock_batch_request
    )

    assert mock_metric_retriever.get_metrics.call_count == 1

    mock_metric_retriever.get_metrics.assert_called_once_with(
        batch_request=mock_batch_request
    )


def test_compute_metric_run_returns_metric_run():
    mock_context = Mock(spec=CloudDataContext)
    mock_metric_retriever = MagicMock(spec=MetricRetriever)

    mock_metric = Mock(spec=TableMetric)
    mock_metric_retriever.get_metrics.return_value = [mock_metric]

    batch_inspector = BatchInspector(
        context=mock_context, metric_retrievers=[mock_metric_retriever]
    )
    mock_batch_request = Mock(spec=BatchRequest)

    data_asset_id = uuid.uuid4()

    metric_run = batch_inspector.compute_metric_run(
        data_asset_id=data_asset_id, batch_request=mock_batch_request
    )

    assert metric_run == MetricRun(
        data_asset_id=data_asset_id,
        metrics=[mock_metric],
    )
