from unittest.mock import Mock
from uuid import UUID

import pandas as pd
import pytest

from great_expectations.data_context import CloudDataContext
from great_expectations.datasource.fluent.interfaces import Batch
from great_expectations.experimental.metric_repository.metric_run_serializer import (
    MetricRunSerializer,
)
from great_expectations.experimental.metric_repository.metrics import (
    ColumnQuantileValuesMetric,
    MetricRun,
)

# Import and use fixtures defined in tests/datasource/fluent/conftest.py
from tests.datasource.fluent.conftest import (
    cloud_api_fake,  # noqa: F401  # used as a fixture
    cloud_details,  # noqa: F401  # used as a fixture
    empty_cloud_context_fluent,  # noqa: F401  # used as a fixture
)


@pytest.fixture
def cloud_context_and_batch_request_with_simple_dataframe(
    empty_cloud_context_fluent: CloudDataContext,  # noqa: F811  # used as a fixture
):
    context = empty_cloud_context_fluent
    datasource = context.sources.add_pandas(name="my_pandas_datasource")

    d = {"col1": [1, 2], "col2": [3, 4]}
    df = pd.DataFrame(data=d)

    name = "dataframe"
    data_asset = datasource.add_dataframe_asset(name=name)
    batch_request = data_asset.build_batch_request(dataframe=df)
    return context, batch_request


@pytest.fixture
def mock_batch():
    return Mock(autospec=Batch)


@pytest.fixture
def batch(cloud_context_and_batch_request_with_simple_dataframe):
    context, batch_request = cloud_context_and_batch_request_with_simple_dataframe
    validator = context.get_validator(batch_request=batch_request)
    batch = validator.active_batch
    return batch


def test_serialize_metric_run(batch: Batch):
    mock_id = UUID("b606af51-df84-49b5-b6a6-aef774b785ac")
    data_asset_id = UUID("4469ed3b-61d4-421f-9635-8339d2558b0f")
    serializer = MetricRunSerializer()
    ColumnQuantileValuesMetric.update_forward_refs()
    metric_run = MetricRun(
        id=mock_id,
        data_asset_id=data_asset_id,
        metrics=[
            ColumnQuantileValuesMetric(
                id=mock_id,
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
    serialized_metric_run = serializer.serialize(metric_run)
    assert serialized_metric_run == (
        f"""{{"id": "{mock_id}", "data_asset_id": "{data_asset_id}", "metrics": [{{"id": "{mock_id}", "batch_id": "batch_id", "metric_name": "metric_name", "value": [0.25, 0.5, 0.75], "exception": null, "column": "column", "quantiles": [0.25, 0.5, 0.75], "allow_relative_error": 0.001}}]}}"""
    )
