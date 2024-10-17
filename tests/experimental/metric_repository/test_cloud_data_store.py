import uuid
from unittest import mock
from unittest.mock import Mock  # noqa: TID251
from uuid import UUID

import numpy
import pytest

from great_expectations.data_context import CloudDataContext
from great_expectations.experimental.metric_repository.cloud_data_store import (
    CloudDataStore,
)
from great_expectations.experimental.metric_repository.metrics import (
    ColumnMetric,
    ColumnQuantileValuesMetric,
    MetricException,
    MetricRun,
)


@pytest.mark.cloud  # NOTE: needs orjson dependency
class TestCloudDataStoreMetricRun:
    def test_add_metric_run_non_generic_metric_type(
        self,
        empty_cloud_context_fluent: CloudDataContext,  # used as a fixture
    ):
        cloud_data_store = CloudDataStore(context=empty_cloud_context_fluent)
        data_asset_id = UUID("4469ed3b-61d4-421f-9635-8339d2558b0f")
        metric_run = MetricRun(
            data_asset_id=data_asset_id,
            metrics=[
                ColumnQuantileValuesMetric(
                    batch_id="batch_id",
                    metric_name="metric_name",
                    value=[0.25, 0.5, 0.75],
                    exception=None,
                    column="column",
                    quantiles=[0.25, 0.5, 0.75],
                    allow_relative_error=0.001,
                )
            ],
        )
        cloud_data_store._session = Mock()
        cloud_data_store._session.post = Mock()
        response_mock = Mock()
        cloud_data_store._session.post.return_value = response_mock

        response_metric_run_id = uuid.uuid4()
        response_metric_id = uuid.uuid4()
        response_mock.json.return_value = {
            "id": str(response_metric_run_id),
            "data_asset_id": str(data_asset_id),
            "metrics": [
                {
                    "id": str(response_metric_id),
                    "metric_type": "ColumnQuantileValuesMetric",
                    "value_type": "list[float]",
                    "allow_relative_error": 0.001,
                    "batch_id": "batch_id",
                    "column": "column",
                    "exception": None,
                    "metric_name": "metric_name",
                    "quantiles": [0.25, 0.5, 0.75],
                    "value": [0.25, 0.5, 0.75],
                }
            ],
        }

        uuid_from_add = cloud_data_store.add(metric_run)

        expected_data = '{"data":{"data_asset_id":"4469ed3b-61d4-421f-9635-8339d2558b0f","metrics":[{"batch_id":"batch_id","metric_name":"metric_name","value":[0.25,0.5,0.75],"exception":null,"column":"column","quantiles":[0.25,0.5,0.75],"allow_relative_error":0.001,"value_type":"list[float]","metric_type":"ColumnQuantileValuesMetric"}]}}'  # noqa: E501

        cloud_data_store._session.post.assert_called_once_with(
            url="https://app.greatexpectations.fake.io/api/v1/organizations/12345678-1234-5678-1234-567812345678/metric-runs",
            data=expected_data,
        )
        assert uuid_from_add == response_metric_run_id

    def test_add_metric_run_generic_metric_type(
        self,
        empty_cloud_context_fluent: CloudDataContext,  # used as a fixture
    ):
        cloud_data_store = CloudDataStore(context=empty_cloud_context_fluent)
        data_asset_id = UUID("4469ed3b-61d4-421f-9635-8339d2558b0f")
        metric_run = MetricRun(
            data_asset_id=data_asset_id,
            metrics=[
                ColumnMetric[int](
                    batch_id="batch_id",
                    metric_name="metric_name",
                    value=1,
                    exception=None,
                    column="column",
                )
            ],
        )
        cloud_data_store._session = Mock()
        cloud_data_store._session.post = Mock()
        response_mock = Mock()
        cloud_data_store._session.post.return_value = response_mock

        response_metric_run_id = uuid.uuid4()
        response_metric_id = uuid.uuid4()
        response_mock.json.return_value = {
            "id": str(response_metric_run_id),
            "data_asset_id": str(data_asset_id),
            "metrics": [
                {
                    "id": str(response_metric_id),
                    "metric_type": "ColumnMetric",
                    "value_type": "int",
                    "batch_id": "batch_id",
                    "column": "column",
                    "exception": None,
                    "metric_name": "metric_name",
                    "value": 1,
                }
            ],
        }

        uuid_from_add = cloud_data_store.add(metric_run)

        expected_data = '{"data":{"data_asset_id":"4469ed3b-61d4-421f-9635-8339d2558b0f","metrics":[{"batch_id":"batch_id","metric_name":"metric_name","value":1,"exception":null,"column":"column","value_type":"int","metric_type":"ColumnMetric"}]}}'  # noqa: E501

        cloud_data_store._session.post.assert_called_once_with(
            url="https://app.greatexpectations.fake.io/api/v1/organizations/12345678-1234-5678-1234-567812345678/metric-runs",
            data=expected_data,
        )
        assert uuid_from_add == response_metric_run_id

    def test_add_metric_run_generic_metric_type_with_exception(
        self,
        empty_cloud_context_fluent: CloudDataContext,  # used as a fixture
    ):
        cloud_data_store = CloudDataStore(context=empty_cloud_context_fluent)
        data_asset_id = UUID("4469ed3b-61d4-421f-9635-8339d2558b0f")
        metric_run = MetricRun(
            data_asset_id=data_asset_id,
            metrics=[
                ColumnMetric[int](
                    batch_id="batch_id",
                    metric_name="metric_name",
                    value=1,
                    exception=MetricException(type="exception type", message="exception message"),
                    column="column",
                )
            ],
        )
        cloud_data_store._session = Mock()
        cloud_data_store._session.post = Mock()
        response_mock = Mock()
        cloud_data_store._session.post.return_value = response_mock

        response_metric_run_id = uuid.uuid4()
        response_metric_id = uuid.uuid4()
        response_mock.json.return_value = {
            "id": str(response_metric_run_id),
            "data_asset_id": str(data_asset_id),
            "metrics": [
                {
                    "id": str(response_metric_id),
                    "metric_type": "ColumnMetric",
                    "value_type": "int",
                    "batch_id": "batch_id",
                    "column": "column",
                    "exception": {
                        "message": "exception message",
                        "type": "exception type",
                    },
                    "metric_name": "metric_name",
                    "value": 1,
                }
            ],
        }

        uuid_from_add = cloud_data_store.add(metric_run)

        expected_data = '{"data":{"data_asset_id":"4469ed3b-61d4-421f-9635-8339d2558b0f","metrics":[{"batch_id":"batch_id","metric_name":"metric_name","value":1,"exception":{"type":"exception type","message":"exception message"},"column":"column","value_type":"int","metric_type":"ColumnMetric"}]}}'  # noqa: E501

        cloud_data_store._session.post.assert_called_once_with(
            url="https://app.greatexpectations.fake.io/api/v1/organizations/12345678-1234-5678-1234-567812345678/metric-runs",
            data=expected_data,
        )
        assert uuid_from_add == response_metric_run_id

    def test_add_metric_run_generic_metric_type_numpy(
        self,
        empty_cloud_context_fluent: CloudDataContext,  # used as a fixture
    ):
        cloud_data_store = CloudDataStore(context=empty_cloud_context_fluent)
        data_asset_id = UUID("4469ed3b-61d4-421f-9635-8339d2558b0f")
        metric_run = MetricRun(
            data_asset_id=data_asset_id,
            metrics=[
                ColumnMetric[numpy.float64](
                    batch_id="batch_id",
                    metric_name="metric_name",
                    value=numpy.float64(2.5),
                    exception=None,
                    column="column",
                )
            ],
        )
        cloud_data_store._session = Mock()
        cloud_data_store._session.post = Mock()
        response_mock = Mock()
        cloud_data_store._session.post.return_value = response_mock

        response_metric_run_id = uuid.uuid4()
        response_metric_id = uuid.uuid4()
        response_mock.json.return_value = {
            "id": str(response_metric_run_id),
            "data_asset_id": str(data_asset_id),
            "metrics": [
                {
                    "id": str(response_metric_id),
                    "metric_type": "ColumnMetric",
                    "value_type": "float64",
                    "batch_id": "batch_id",
                    "column": "column",
                    "exception": None,
                    "metric_name": "metric_name",
                    "value": 2.5,
                }
            ],
        }

        uuid_from_add = cloud_data_store.add(metric_run)

        expected_data = '{"data":{"data_asset_id":"4469ed3b-61d4-421f-9635-8339d2558b0f","metrics":[{"batch_id":"batch_id","metric_name":"metric_name","value":2.5,"exception":null,"column":"column","value_type":"float64","metric_type":"ColumnMetric"}]}}'  # noqa: E501

        cloud_data_store._session.post.assert_called_once_with(
            url="https://app.greatexpectations.fake.io/api/v1/organizations/12345678-1234-5678-1234-567812345678/metric-runs",
            data=expected_data,
        )
        assert uuid_from_add == response_metric_run_id


@pytest.mark.cloud
def test_closes_session(
    empty_cloud_context_fluent: CloudDataContext,  # used as a fixture
):
    """Make sure that when the CloudDataStore object is garbage collected,
    the session is closed."""
    cloud_data_store = CloudDataStore(context=empty_cloud_context_fluent)
    with mock.patch("requests.Session.close", autospec=True) as mock_close:
        # Use the finalizer to remove the object from memory.
        # Using del or __del__ directly does not work in the test environment.
        cloud_data_store._finalizer()
        mock_close.assert_called_once()
