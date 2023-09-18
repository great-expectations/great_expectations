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
    ColumnMetric,
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

    d = {
        "numeric_with_nulls_1": [1, 2, None],
        "numeric_with_nulls_2": [3, 4, None],
        "string": ["a", "b", "c"],
        "string_with_nulls": ["a", "b", None],
        "boolean": [True, False, True],
        "datetime": [
            pd.to_datetime("2020-01-01"),
            pd.to_datetime("2020-01-02"),
            pd.to_datetime("2020-01-03"),
        ],
    }
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

    # Note: expected_metrics needs to be in the same order as metrics for the assert statement.
    expected_metrics = [
        TableMetric[int](
            batch_id=batch_id,
            metric_name="table.row_count",
            value=3,
            exception=None,
        ),
        TableMetric[List[str]](
            batch_id=batch_id,
            metric_name="table.columns",
            value=[
                "numeric_with_nulls_1",
                "numeric_with_nulls_2",
                "string",
                "string_with_nulls",
                "boolean",
                "datetime",
            ],
            exception=None,
        ),
        ColumnMetric[float](
            batch_id=batch_id,
            metric_name="column.min",
            column="numeric_with_nulls_1",
            value=1,
            exception=None,
        ),
        ColumnMetric[float](
            batch_id=batch_id,
            metric_name="column.min",
            column="numeric_with_nulls_2",
            value=3,
            exception=None,
        ),
        ColumnMetric[float](
            batch_id=batch_id,
            metric_name="column.max",
            column="numeric_with_nulls_1",
            value=2,
            exception=None,
        ),
        ColumnMetric[float](
            batch_id=batch_id,
            metric_name="column.max",
            column="numeric_with_nulls_2",
            value=4,
            exception=None,
        ),
        ColumnMetric[float](
            batch_id=batch_id,
            metric_name="column.mean",
            column="numeric_with_nulls_1",
            value=1.5,
            exception=None,
        ),
        ColumnMetric[float](
            batch_id=batch_id,
            metric_name="column.mean",
            column="numeric_with_nulls_2",
            value=3.5,
            exception=None,
        ),
        ColumnMetric[float](
            batch_id=batch_id,
            metric_name="column.median",
            column="numeric_with_nulls_1",
            value=1.5,
            exception=None,
        ),
        ColumnMetric[float](
            batch_id=batch_id,
            metric_name="column.median",
            column="numeric_with_nulls_2",
            value=3.5,
            exception=None,
        ),
        TableMetric[List[str]](
            batch_id=batch_id,
            metric_name="table.column_types",
            value=[
                {"name": "numeric_with_nulls_1", "type": "float64"},
                {"name": "numeric_with_nulls_2", "type": "float64"},
                {"name": "string", "type": "object"},
                {"name": "string_with_nulls", "type": "object"},
                {"name": "boolean", "type": "bool"},
                {"name": "datetime", "type": "datetime64[ns]"},
            ],
            exception=None,
        ),
        ColumnMetric[int](
            batch_id=batch_id,
            metric_name="column_values.null.count",
            column="numeric_with_nulls_1",
            value=1,
            exception=None,
        ),
        ColumnMetric[int](
            batch_id=batch_id,
            metric_name="column_values.null.count",
            column="numeric_with_nulls_2",
            value=1,
            exception=None,
        ),
        ColumnMetric[int](
            batch_id=batch_id,
            metric_name="column_values.null.count",
            column="string",
            value=0,
            exception=None,
        ),
        ColumnMetric[int](
            batch_id=batch_id,
            metric_name="column_values.null.count",
            column="string_with_nulls",
            value=1,
            exception=None,
        ),
        ColumnMetric[int](
            batch_id=batch_id,
            metric_name="column_values.null.count",
            column="boolean",
            value=0,
            exception=None,
        ),
        ColumnMetric[int](
            batch_id=batch_id,
            metric_name="column_values.null.count",
            column="datetime",
            value=0,
            exception=None,
        ),
    ]

    # Assert each metric so it is easier to see which one fails (instead of assert metrics == expected_metrics):
    assert len(metrics) == len(expected_metrics)
    for metric in metrics:
        assert metric.dict() in [
            expected_metric.dict() for expected_metric in expected_metrics
        ]
