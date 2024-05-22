"""Test using actual sample data."""

from __future__ import annotations

from typing import List

import pandas as pd
import pytest
from pandas import Timestamp

from great_expectations.data_context import CloudDataContext
from great_expectations.datasource.fluent.batch_request import BatchRequest
from great_expectations.experimental.metric_repository.metric_list_metric_retriever import (
    MetricListMetricRetriever,
)
from great_expectations.experimental.metric_repository.metrics import (
    ColumnMetric,
    MetricTypes,
    TableMetric,
)


@pytest.fixture
def cloud_context_and_batch_request_with_simple_dataframe(
    empty_cloud_context_fluent: CloudDataContext,  # used as a fixture
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
def test_get_metrics_table_metrics_only(
    cloud_context_and_batch_request_with_simple_dataframe: tuple[
        CloudDataContext, BatchRequest
    ],
):
    context, batch_request = cloud_context_and_batch_request_with_simple_dataframe
    table_metrics_list: List[MetricTypes] = [
        MetricTypes.TABLE_ROW_COUNT,
        MetricTypes.TABLE_COLUMNS,
        MetricTypes.TABLE_COLUMN_TYPES,
    ]
    metric_retriever = MetricListMetricRetriever(context)
    metrics = metric_retriever.get_metrics(
        batch_request=batch_request, metric_list=table_metrics_list
    )
    validator = context.get_validator(batch_request=batch_request)
    batch_id = validator.active_batch.id

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
    ]

    # Assert each metric so it is easier to see which one fails (instead of assert metrics == expected_metrics):
    assert len(metrics) == len(expected_metrics)
    for metric in metrics:
        assert metric.dict() in [
            expected_metric.dict() for expected_metric in expected_metrics
        ]


@pytest.mark.cloud
def test_get_metrics_full_cdm(
    cloud_context_and_batch_request_with_simple_dataframe: tuple[
        CloudDataContext, BatchRequest
    ],
):
    context, batch_request = cloud_context_and_batch_request_with_simple_dataframe
    cdm_metrics_list: List[MetricTypes] = [
        MetricTypes.TABLE_ROW_COUNT,
        MetricTypes.TABLE_COLUMNS,
        MetricTypes.TABLE_COLUMN_TYPES,
        MetricTypes.COLUMN_MIN,
        MetricTypes.COLUMN_MAX,
        MetricTypes.COLUMN_MEAN,
        MetricTypes.COLUMN_MEDIAN,
        MetricTypes.COLUMN_NULL_COUNT,
    ]
    metric_retriever = MetricListMetricRetriever(context)
    metrics = metric_retriever.get_metrics(
        batch_request=batch_request, metric_list=cdm_metrics_list
    )
    validator = context.get_validator(batch_request=batch_request)
    batch_id = validator.active_batch.id

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
        ColumnMetric[str](
            batch_id=batch_id,
            metric_name="column.min",
            value=Timestamp("2020-01-01 00:00:00"),
            exception=None,
            column="datetime",
        ),
        ColumnMetric[str](
            batch_id=batch_id,
            metric_name="column.max",
            value=Timestamp("2020-01-03 00:00:00"),
            exception=None,
            column="datetime",
        ),
    ]

    assert len(metrics) == len(expected_metrics)
    for metric in metrics:
        assert metric.dict() in [
            expected_metric.dict() for expected_metric in expected_metrics
        ]
