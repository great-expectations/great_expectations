from __future__ import annotations

from typing import List

from great_expectations.agent.actions import ActionResult
from great_expectations.experimental.metric_repository.cloud_data_store import (
    CloudDataStore,
)

"""
This file contains demo code for the column_descriptive_metrics module.
Unit, integration and end-to-end tests should be written to replace this code.
"""
import uuid
from unittest import mock

import pandas as pd
import pytest

from great_expectations.agent.actions.run_column_descriptive_metrics_action import (
    ColumnDescriptiveMetricsAction,
)
from great_expectations.agent.models import (
    CreatedResource,
    RunColumnDescriptiveMetricsEvent,
)
from great_expectations.data_context import CloudDataContext
from great_expectations.datasource.fluent.batch_request import BatchRequest
from great_expectations.experimental.metric_repository.batch_inspector import (
    BatchInspector,
)
from great_expectations.experimental.metric_repository.column_descriptive_metrics_metric_retriever import (
    ColumnDescriptiveMetricsMetricRetriever,
)
from great_expectations.experimental.metric_repository.metric_repository import (
    MetricRepository,
)
from great_expectations.experimental.metric_repository.metrics import (
    Metric,
    MetricException,
    MetricRun,
    TableMetric,
)

# Import and use fixtures defined in tests/datasource/fluent/conftest.py
from tests.datasource.fluent.conftest import (
    cloud_api_fake,  # noqa: F401  # used as a fixture
    cloud_details,  # noqa: F401  # used as a fixture
    empty_cloud_context_fluent,  # noqa: F401  # used as a fixture
)


@pytest.fixture
def metric_id() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def run_id() -> uuid.UUID:
    return uuid.uuid4()


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


# @pytest.mark.xfail(
#     reason="This test is meant as a demo during development and currently fails due to differing batch data object ids and ge_load_time batch marker"
# )
def test_demo_batch_inspector(
    metric_id: uuid.UUID,
    run_id: uuid.UUID,
    cloud_context_and_batch_request_with_simple_dataframe: tuple[
        CloudDataContext, BatchRequest
    ],
):
    """This is a demo of how to get column descriptive metrics,
    this should be replaced with proper tests."""

    context, batch_request = cloud_context_and_batch_request_with_simple_dataframe

    event = RunColumnDescriptiveMetricsEvent(
        datasource_name=batch_request.datasource_name,
        data_asset_name=batch_request.data_asset_name,
    )

    metric_retriever = ColumnDescriptiveMetricsMetricRetriever(context)
    metric_retrievers = [metric_retriever]
    batch_inspector = BatchInspector(context, metric_retrievers)
    cloud_data_store = CloudDataStore(context)
    column_descriptive_metrics_repository = MetricRepository(
        data_store=cloud_data_store
    )
    action = ColumnDescriptiveMetricsAction(
        context=context,
        batch_inspector=batch_inspector,
        metric_repository=column_descriptive_metrics_repository,
    )

    # Override generate id methods to return consistent ids for the test
    batch_inspector._generate_run_id = lambda: run_id
    metric_retriever._generate_metric_id = lambda: metric_id

    with mock.patch(
        f"{MetricRepository.__module__}.{MetricRepository.__name__}.add",
    ) as mock_add:
        run_result = action.run(event, "some_event_id")

    metrics_stored = mock_add.call_args[0][0]

    # Get pointer to batch
    validator = context.get_validator(batch_request=batch_request)
    batch = validator.active_batch

    assert metrics_stored == MetricRun(
        id=run_id,
        metrics=[
            TableMetric[int](
                id=metric_id,
                batch=batch,
                metric_name="table.row_count",
                value=2,
                exception=MetricException(),
            ),
            TableMetric[List[str]](
                id=metric_id,
                batch=batch,
                metric_name="table.columns",
                value=["col1", "col2"],
                exception=MetricException(),
            ),
        ],
    )

    expected_action_result = ActionResult(
        id="some_event_id",
        type=event.type,
        created_resources=[
            CreatedResource(resource_id=str(run_id), type="MetricRun"),
        ],
    )
    assert run_result == expected_action_result


@pytest.mark.parametrize(
    "metric_type",
    [
        Metric,
    ],
)
def test_cannot_init_abstract_metric(
    metric_id: uuid.UUID,
    run_id: uuid.UUID,
    cloud_context_and_batch_request_with_simple_dataframe: tuple[
        CloudDataContext, BatchRequest
    ],
    metric_type: type(Metric),
):
    """This is a demo, we should implement better tests e.g. mocking Batch."""

    context, batch_request = cloud_context_and_batch_request_with_simple_dataframe

    # Get pointer to batch
    validator = context.get_validator(batch_request=batch_request)
    batch = validator.active_batch

    metric_type.update_forward_refs()

    with pytest.raises(NotImplementedError):
        metric_type(
            id=metric_id,
            run_id=run_id,
            batch=batch,
            metric_name="table.columns",
            exception=MetricException(),
        )
