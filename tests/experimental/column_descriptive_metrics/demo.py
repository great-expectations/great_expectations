"""
This file contains demo code for the column_descriptive_metrics module.
Unit, integration and end-to-end tests should be written to replace this code.
"""
import uuid
from unittest import mock

import pytest
from great_expectations.experimental.column_descriptive_metrics.batch_inspector import (
    BatchInspector,
)
from great_expectations.experimental.column_descriptive_metrics.metric_converter import (
    MetricConverter,
)
from great_expectations.datasource.fluent.batch_request import BatchRequest

import pandas as pd
import great_expectations as gx
from great_expectations.experimental.column_descriptive_metrics.metrics import (
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
def ephemeral_context_with_simple_dataframe():
    context = gx.get_context()
    datasource = context.sources.add_pandas(name="my_pandas_datasource")

    d = {"col1": [1, 2], "col2": [3, 4]}
    df = pd.DataFrame(data=d)

    name = "dataframe"
    data_asset = datasource.add_dataframe_asset(name=name)
    batch_request = data_asset.build_batch_request(dataframe=df)
    return context, batch_request


@pytest.fixture
def ephemeral_context_with_simple_dataframe_and_validator(
    ephemeral_context_with_simple_dataframe,
):
    context, batch_request = ephemeral_context_with_simple_dataframe
    validator = context.get_validator(
        datasource_name=batch_request.datasource_name,
        data_asset_name=batch_request.data_asset_name,
        batch_request=batch_request,
    )
    return context, batch_request, validator


def _create_mock_agent_action(batch_request: BatchRequest):
    class MockAgentAction:
        def __init__(self, datasource_name, data_asset_name):
            self.datasource_name = datasource_name
            self.data_asset_name = data_asset_name

    mock_agent_action = MockAgentAction(
        batch_request.datasource_name, batch_request.data_asset_name
    )
    return mock_agent_action


def test_demo_batch_inspector(
    cloud_org_id: uuid.UUID,
    metric_id: uuid.UUID,
    run_id: uuid.UUID,
    ephemeral_context_with_simple_dataframe_and_validator,
):
    """This is a demo of how to get column descriptive metrics,
    this should be replaced with proper tests."""

    (
        context,
        batch_request,
        validator,
    ) = ephemeral_context_with_simple_dataframe_and_validator

    # From here down assume we just have the batch request from the agent action
    # (using the datasource and data asset names from the batch request for convenience):
    mock_agent_action = _create_mock_agent_action(batch_request)

    datasource_from_action = context.get_datasource(mock_agent_action.datasource_name)

    data_asset_from_action = datasource_from_action.get_asset(
        mock_agent_action.data_asset_name
    )
    batch_request_from_action = data_asset_from_action.build_batch_request()
    validator_from_action = context.get_validator(
        batch_request=batch_request_from_action
    )

    # TODO: Use real event and action.run() instead of reproducting that code here.
    #  (first address circular import issues)
    # event = RunOnboardingDataAssistantEvent(
    #     datasource_name=batch_request.datasource_name,
    #     data_asset_name=batch_request.data_asset_name,
    # )
    # action = RunOnboardingDataAssistantAction(event)

    with mock.patch(
        f"{BatchInspector.__module__}.{BatchInspector.__name__}._generate_run_id",
        return_value=run_id,
    ), mock.patch(
        f"{MetricConverter.__module__}.{MetricConverter.__name__}._generate_metric_id",
        return_value=metric_id,
    ):
        metric_converter = MetricConverter(organization_id=cloud_org_id)
        batch_inspector = BatchInspector(metric_converter)
        metrics = batch_inspector.get_column_descriptive_metrics(
            validator=validator_from_action
        )

    assert metrics == Metrics(
        metrics=[
            Metric(
                id=metric_id,
                organization_id=cloud_org_id,
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
                organization_id=cloud_org_id,
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
        ]
    )
