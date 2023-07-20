from __future__ import annotations

"""
This file contains demo code for the column_descriptive_metrics module.
Unit, integration and end-to-end tests should be written to replace this code.
"""
import uuid
from unittest import mock

import pytest

from great_expectations.agent.models import RunBatchInspectorEvent
from great_expectations.data_context import CloudDataContext
from great_expectations.experimental.metric_repository.batch_inspector import (
    BatchInspector,
)
from great_expectations.experimental.metric_repository.batch_inspector_agent_action import (
    RunBatchInspectorAction,
)
from great_expectations.experimental.metric_repository.metric_repository import (
    MetricRepository,
)
from great_expectations.datasource.fluent.batch_request import BatchRequest

import pandas as pd
from great_expectations.experimental.metric_repository.metrics import (
    Metric,
    Value,
    Metrics,
)


@pytest.fixture
def cloud_org_id() -> uuid.UUID:
    return uuid.UUID("6e9c4af6-7616-43c3-9b40-a1a9e100c4a0")


@pytest.fixture
def metric_id() -> uuid.UUID:
    return uuid.UUID("0fa72ac7-df72-4bf2-9fe5-e6b01eed6e95")


@pytest.fixture
def run_id() -> uuid.UUID:
    return uuid.UUID("2a24847b-dae4-43c7-8027-1bd1cd572690")


@pytest.fixture
def cloud_context_and_batch_request_with_simple_dataframe(
    empty_cloud_context_fluent: CloudDataContext,
):
    context = empty_cloud_context_fluent
    datasource = context.sources.add_pandas(name="my_pandas_datasource")

    d = {"col1": [1, 2], "col2": [3, 4]}
    df = pd.DataFrame(data=d)

    name = "dataframe"
    data_asset = datasource.add_dataframe_asset(name=name)
    batch_request = data_asset.build_batch_request(dataframe=df)
    return context, batch_request


def test_demo_batch_inspector(
    cloud_org_id: uuid.UUID,
    metric_id: uuid.UUID,
    run_id: uuid.UUID,
    cloud_context_and_batch_request_with_simple_dataframe: tuple[
        CloudDataContext, BatchRequest
    ],
):
    """This is a demo of how to get column descriptive metrics,
    this should be replaced with proper tests."""

    context, batch_request = cloud_context_and_batch_request_with_simple_dataframe

    event = RunBatchInspectorEvent(
        datasource_name=batch_request.datasource_name,
        data_asset_name=batch_request.data_asset_name,
    )
    action = RunBatchInspectorAction(context)

    with mock.patch(
        f"{BatchInspector.__module__}.{BatchInspector.__name__}._generate_run_id",
        return_value=run_id,
    ), mock.patch(
        f"{BatchInspector.__module__}.{BatchInspector.__name__}._generate_metric_id",
        return_value=metric_id,
    ), mock.patch(
        f"{MetricRepository.__module__}.{MetricRepository.__name__}.create",
    ) as mock_create:
        action.run(event, "some_event_id")

    metrics_stored = mock_create.call_args[0][0]

    assert metrics_stored == Metrics(
        id=run_id,
        metrics=[
            Metric(
                id=metric_id,
                run_id=run_id,
                # TODO: reimplement batch param
                # batch=batch_from_action,
                metric_name="table.row_count",
                metric_domain_kwargs={},
                metric_value_kwargs={},
                column=None,
                value=Value(value=2),
                details={},
            ),
            Metric(
                id=metric_id,
                run_id=run_id,
                # TODO: reimplement batch param
                # batch=batch_from_action,
                metric_name="table.columns",
                metric_domain_kwargs={},
                metric_value_kwargs={},
                column=None,
                value=Value(value=["col1", "col2"]),
                details={},
            ),
        ],
    )
