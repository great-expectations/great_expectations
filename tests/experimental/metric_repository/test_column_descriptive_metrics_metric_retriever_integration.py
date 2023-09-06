"""Test using actual sample data."""
from __future__ import annotations

from typing import List

import pandas as pd
import pytest

from great_expectations.data_context import CloudDataContext
from great_expectations.datasource.fluent.batch_request import BatchRequest
from great_expectations.experimental.metric_repository.column_descriptive_metrics_metric_retriever import (
    ColumnDescriptiveMetricsMetricRetriever,
)
from great_expectations.experimental.metric_repository.metrics import (
    TableMetric,
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


@pytest.mark.cloud
def test_get_metrics(
    cloud_context_and_batch_request_with_simple_dataframe: tuple[
        CloudDataContext, BatchRequest
    ],
):
    context, batch_request = cloud_context_and_batch_request_with_simple_dataframe

    metric_retriever = ColumnDescriptiveMetricsMetricRetriever(context)
    metrics = metric_retriever.get_metrics(batch_request=batch_request)
    validator = context.get_validator(batch_request=batch_request)
    batch_id = validator.active_batch.id
    assert metrics == [
        TableMetric[int](
            batch_id=batch_id,
            metric_name="table.row_count",
            value=2,
            exception=None,
        ),
        TableMetric[List[str]](
            batch_id=batch_id,
            metric_name="table.columns",
            value=["col1", "col2"],
            exception=None,
        ),
    ]
