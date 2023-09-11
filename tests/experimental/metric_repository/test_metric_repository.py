from unittest.mock import Mock

import pytest

from great_expectations.experimental.metric_repository.data_store import DataStore
from great_expectations.experimental.metric_repository.metric_repository import (
    MetricRepository,
)
from great_expectations.experimental.metric_repository.metrics import MetricRun


@pytest.fixture
def mock_data_store():
    return Mock(autospec=DataStore)


@pytest.fixture
def mock_metric_run():
    return Mock(autospec=MetricRun)


@pytest.mark.unit
def test_add_metric_run(mock_data_store: DataStore, mock_metric_run: MetricRun):
    metric_repository = MetricRepository(data_store=mock_data_store)

    metric_repository.add_metric_run(metric_run=mock_metric_run)

    mock_data_store.add.assert_called_once_with(value=mock_metric_run)
