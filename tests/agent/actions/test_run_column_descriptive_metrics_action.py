import uuid
from unittest.mock import Mock

import pytest

from great_expectations.agent.actions import ColumnDescriptiveMetricsAction
from great_expectations.agent.models import (
    CreatedResource,
    RunColumnDescriptiveMetricsEvent,
)
from great_expectations.data_context import CloudDataContext
from great_expectations.experimental.metric_repository.batch_inspector import (
    BatchInspector,
)
from great_expectations.experimental.metric_repository.metric_repository import (
    MetricRepository,
)
from great_expectations.experimental.metric_repository.metrics import MetricRun

pytestmark = pytest.mark.unit


def test_run_column_descriptive_metrics_computes_metric_run():
    mock_context = Mock(spec=CloudDataContext)
    mock_metric_repository = Mock(spec=MetricRepository)
    mock_batch_inspector = Mock(spec=BatchInspector)

    action = ColumnDescriptiveMetricsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
    )

    action.run(
        event=RunColumnDescriptiveMetricsEvent(
            type="column_descriptive_metrics_request.received",
            datasource_name="test-datasource",
            data_asset_name="test-data-asset",
        ),
        id="test-id",
    )

    mock_batch_inspector.compute_metric_run.assert_called_once()


def test_run_column_descriptive_metrics_creates_metric_run():
    mock_context = Mock(spec=CloudDataContext)
    mock_metric_repository = Mock(spec=MetricRepository)
    mock_batch_inspector = Mock(spec=BatchInspector)

    mock_metric_run = Mock(spec=MetricRun)
    mock_batch_inspector.compute_metric_run.return_value = mock_metric_run

    action = ColumnDescriptiveMetricsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
    )

    action.run(
        event=RunColumnDescriptiveMetricsEvent(
            type="column_descriptive_metrics_request.received",
            datasource_name="test-datasource",
            data_asset_name="test-data-asset",
        ),
        id="test-id",
    )

    mock_metric_repository.add_metric_run.assert_called_once_with(mock_metric_run)


def test_run_column_descriptive_metrics_returns_action_result():
    mock_context = Mock(spec=CloudDataContext)
    mock_metric_repository = Mock(spec=MetricRepository)
    mock_batch_inspector = Mock(spec=BatchInspector)

    metric_run_id = uuid.uuid4()
    mock_metric_repository.add_metric_run.return_value = metric_run_id

    action = ColumnDescriptiveMetricsAction(
        context=mock_context,
        metric_repository=mock_metric_repository,
        batch_inspector=mock_batch_inspector,
    )

    action_result = action.run(
        event=RunColumnDescriptiveMetricsEvent(
            type="column_descriptive_metrics_request.received",
            datasource_name="test-datasource",
            data_asset_name="test-data-asset",
        ),
        id="test-id",
    )

    assert action_result.type == "column_descriptive_metrics_request.received"
    assert action_result.id == "test-id"
    assert action_result.created_resources == [
        CreatedResource(resource_id=str(metric_run_id), type="MetricRun"),
    ]
